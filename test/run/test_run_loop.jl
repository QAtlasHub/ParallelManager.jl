using SweepRunner, Test, DataVault, ParamIO

const FIXTURE_CFG_RL = joinpath(@__DIR__, "fixtures", "study.toml")

function with_vault_rl(f; run::AbstractString="phase1")
    outdir = mktempdir()
    try
        v = DataVault.Vault(FIXTURE_CFG_RL; run=run, outdir=outdir)
        f(v, outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

allk_rl(v) = ParamIO.expand(v.spec)

@testset "run_loop!: all work done in first round, exits immediately" begin
    wf = k -> Dict{String,Any}("x" => 1)
    # The first `run_loop!` in a process pays for compiling the whole `run!` path, which is
    # seconds and is not what the bound below is about. Pay it on another vault, with the same
    # work function, so the timed call measures the loop.
    with_vault_rl() do w, _
        run_loop!(wf, w, allk_rl(w); max_empty_rounds=1, idle_sleep=0.1)
    end
    with_vault_rl() do v, outdir
        keys = allk_rl(v)
        t0 = time()
        run_loop!(
            wf,
            v,
            keys;
            max_empty_rounds=3,
            idle_sleep=0.1,  # short for testing
        )
        elapsed = time() - t0
        # Should finish quickly (not wait 3 idle rounds)
        @test elapsed < 5.0
        for k in keys
            @test DataVault.is_done(v, k)
        end
    end
end

@testset "run_loop!: exits after max_empty_rounds of no new work" begin
    with_vault_rl() do v, outdir
        keys = allk_rl(v)
        # Pre-complete all keys via manifest
        m = load_manifest(v)
        for k in keys
            add_complete!(m, k)
        end
        save_manifest(m)
        # Also mark done in DataVault
        for k in keys
            DataVault.mark_done!(v, k)
        end

        t0 = time()
        run_loop!(
            k -> Dict{String,Any}("x" => 1), v, keys; max_empty_rounds=2, idle_sleep=0.1
        )
        elapsed = time() - t0
        # Should exit after finding no work (early skip each round)
        @test elapsed < 2.0
    end
end

@testset "run_loop!: stop_flag causes immediate exit" begin
    with_vault_rl() do v, outdir
        keys = allk_rl(v)
        stop = joinpath(outdir, "STOP")
        touch(stop)

        t0 = time()
        run_loop!(
            k -> Dict{String,Any}("x" => 1),
            v,
            keys;
            opts=RunOpts(stop_flag=stop),
            max_empty_rounds=3,
            idle_sleep=0.1,
        )
        elapsed = time() - t0
        @test elapsed < 2.0
    end
end
