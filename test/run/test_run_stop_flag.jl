using ParallelManager, Test, DataVault, ParamIO, JSON3

const FIXTURE_CFG_S = joinpath(@__DIR__, "fixtures", "study.toml")

function with_vault_s(f; run::AbstractString="phase1")
    outdir = mktempdir()
    try
        v = DataVault.Vault(FIXTURE_CFG_S; run=run, outdir=outdir)
        f(v, outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

function _read_event_lines_s(outdir)
    logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir(outdir))
    return vcat([readlines(joinpath(outdir, f)) for f in logs]...)
end

allk_s(v) = ParamIO.expand(v.spec)

@testset "run! + stop_flag: pre-existing flag causes immediate return" begin
    with_vault_s() do v, outdir
        keys = allk_s(v)
        stop = joinpath(outdir, "STOP_NOW")
        touch(stop)

        counter = Ref(0)
        work_fn = k -> (counter[] += 1; Dict{String,Any}("x" => 1))
        result = run!(work_fn, v, keys; opts=RunOpts(stop_flag=stop))

        # No keys should be processed — stop was already set
        @test counter[] == 0
        @test result.done == 0
    end
end

@testset "run! + stop_flag: flag raised mid-run causes partial results" begin
    with_vault_s() do v, outdir
        keys = allk_s(v)
        @test length(keys) >= 2
        stop = joinpath(outdir, "STOP_NOW")

        call_count = Ref(0)
        work_fn = k -> begin
            call_count[] += 1
            if call_count[] == 1
                # After processing the first key, raise the stop flag
                touch(stop)
            end
            sleep(0.01)
            Dict{String,Any}("x" => 1)
        end
        result = run!(work_fn, v, keys; opts=RunOpts(stop_flag=stop))

        # At least 1 key was done, but not all
        @test result.done >= 1
        @test result.done < length(keys)
    end
end

@testset "run! + stop_flag: no flag means normal execution" begin
    with_vault_s() do v, outdir
        keys = allk_s(v)
        result = run!(
            k -> Dict{String,Any}("x" => 1), v, keys; opts=RunOpts(stop_flag=nothing)
        )
        @test result.done == length(keys)
    end
end
