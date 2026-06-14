using ParallelManager, Test, DataVault, ParamIO, JSON3

const FIXTURE_CFG_M = joinpath(@__DIR__, "fixtures", "study.toml")

function _read_event_lines_m(outdir)
    logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir(outdir))
    return vcat([readlines(joinpath(outdir, f)) for f in logs]...)
end

function with_vault_m(f; run::AbstractString="phase1")
    outdir = mktempdir()
    try
        v = DataVault.Vault(FIXTURE_CFG_M; run=run, outdir=outdir)
        f(v, outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

allkeys(v::DataVault.Vault) = ParamIO.expand(v.spec)

@testset "run! + manifest: first run creates manifest" begin
    with_vault_m() do v, outdir
        keys = allkeys(v)
        result = run!(key -> Dict{String,Any}("x" => 1), v, keys)
        @test result.done == length(keys)

        mpath = manifest_path(manifest_root(v), Symbol(v.run))
        @test isfile(mpath)

        m = load_manifest(v)
        @test length(m.complete) == length(keys)
        for k in keys
            @test is_complete(m, k)
        end
    end
end

@testset "run! + manifest: second run is early-skip" begin
    with_vault_m() do v, outdir
        keys = allkeys(v)
        counter = Ref(0)
        work_fn = _ -> (counter[] += 1; Dict{String,Any}("x" => 1))

        run!(work_fn, v, keys)
        first_count = counter[]
        @test first_count == length(keys)

        t0 = time()
        result2 = run!(work_fn, v, keys)
        elapsed = time() - t0
        @test counter[] == first_count  # work_fn NOT called again
        @test result2.skipped == length(keys)
        @test result2.done == 0
        @test elapsed < 1.0  # early skip is fast

        # events.jsonl has :skip_complete
        lines = _read_event_lines_m(outdir)
        kinds = [JSON3.read(l).kind for l in lines]
        @test "skip_complete" in kinds
    end
end

@testset "run! + manifest: adding a new key only runs that one" begin
    with_vault_m() do v, outdir
        keys = allkeys(v)
        run!(key -> Dict{String,Any}("x" => 1), v, keys)

        counter = Ref(0)
        # Simulate a new key added later (fake — DataKey with a new param)
        new_key = DataKey(Dict{String,Any}("N" => 99, "J" => 9.9), 1)
        all_keys_plus = vcat(keys, new_key)

        work_fn = k -> begin
            counter[] += 1
            Dict{String,Any}("x" => 2)
        end
        result = run!(work_fn, v, all_keys_plus)
        @test counter[] == 1  # only the new key
        @test result.done == 1
        @test result.skipped == length(keys)
    end
end

@testset "run! + manifest: failed key not persisted" begin
    with_vault_m() do v, outdir
        keys = allkeys(v)
        bad = keys[1]
        work_fn = k -> k == bad ? error("nope") : Dict{String,Any}("x" => 1)
        run!(work_fn, v, keys)

        m = load_manifest(v)
        @test !is_complete(m, bad)
        @test length(m.complete) == length(keys) - 1

        # On re-run the bad key is retried
        counter = Ref(0)
        work_fn2 = k -> (counter[] += 1; Dict{String,Any}("x" => 2))
        result2 = run!(work_fn2, v, keys)
        @test counter[] == 1  # only the previously-bad key
        @test result2.done == 1
    end
end

@testset "run! + manifest: 3600 key early-skip bench" begin
    with_vault_m(; run="bench") do v, outdir
        # Fabricate 3600 keys (don't actually run work_fn 3600 times with full
        # DataVault save; instead preload the manifest directly and measure skip)
        keys = [DataKey(Dict{String,Any}("N" => i, "J" => 1.0), 1) for i in 1:3600]
        m = load_manifest(v)
        for k in keys
            add_complete!(m, k)
        end
        save_manifest(m)

        # Second call should be near-instant
        t0 = time()
        result = run!(k -> Dict{String,Any}("x" => 1), v, keys)
        elapsed = time() - t0
        @test result.skipped == 3600
        @test result.done == 0
        @test elapsed < 1.0  # target: full-done run finishes in <1s
        @info "3600-key early-skip bench" elapsed_s=elapsed
    end
end
