using ParallelManager, Test, DataVault, ParamIO, JSON3

const FIXTURE_CFG = joinpath(@__DIR__, "fixtures", "study.toml")

# Helper: read all lines from per-master event log files in a directory
function _read_event_lines(outdir)
    logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir(outdir))
    return vcat([readlines(joinpath(outdir, f)) for f in logs]...)
end

function with_run_vault(f; run::AbstractString="phase1")
    outdir = mktempdir()
    try
        v = DataVault.Vault(FIXTURE_CFG; run=run, outdir=outdir)
        f(v, outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

function all_keys(v::DataVault.Vault)
    return ParamIO.expand(v.spec)
end

@testset "run! minimal: happy path" begin
    with_run_vault() do v, outdir
        keys = all_keys(v)
        @test length(keys) > 0

        counter = Ref(0)
        work_fn =
            key -> begin
                counter[] += 1
                return Dict{String,Any}("result" => counter[], "N" => key.params["N"])
            end

        result = run!(work_fn, v, keys)
        @test result.total == length(keys)
        @test result.done == length(keys)
        @test result.err == 0

        # Every key is marked done
        for k in keys
            @test DataVault.is_done(v, k)
        end

        # Events are recorded
        lines = _read_event_lines(outdir)
        @test !isempty(lines)
        kinds = [JSON3.read(l).kind for l in lines]
        @test "stage_start" in kinds
        @test "stage_done" in kinds
        # key_start is :debug — suppressed by default; key_done (:info) is the per-key signal
        @test count(==("key_start"), kinds) == 0
        @test count(==("key_done"), kinds) == length(keys)
    end
end

@testset "run! minimal: exception in one key, others complete" begin
    with_run_vault() do v, outdir
        keys = all_keys(v)
        bad_key = keys[2]
        work_fn = key -> begin
            key == bad_key && error("nope")
            return Dict{String,Any}("ok" => true)
        end

        result = run!(work_fn, v, keys)
        @test result.done == length(keys) - 1
        @test result.err == 1
        @test !DataVault.is_done(v, bad_key)
        for k in keys
            k == bad_key && continue
            @test DataVault.is_done(v, k)
        end

        lines = _read_event_lines(outdir)
        kinds = [JSON3.read(l).kind for l in lines]
        @test "error" in kinds
    end
end

@testset "run! minimal: non-Dict payload raises" begin
    with_run_vault() do v, outdir
        keys = all_keys(v)[1:1]
        result = run!(key -> 42, v, keys)
        @test result.err == 1
        @test result.done == 0
    end
end

@testset "run! minimal: no per-key println noise" begin
    # Structural: there must be no println call in Run.jl per-key path.
    src = read(joinpath(pkgdir(ParallelManager), "src", "Run.jl"), String)
    # Strip comments/docstrings to count real println calls
    code_only = replace(src, r"#[^\n]*" => "")
    @test !occursin("println(", code_only)
end
