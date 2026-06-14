using ParallelManager, Test, DataVault, ParamIO, JSON3

const FIXTURE_CFG_L = joinpath(@__DIR__, "fixtures", "study.toml")

function _read_event_lines_l(outdir)
    logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir(outdir))
    return vcat([readlines(joinpath(outdir, f)) for f in logs]...)
end

function with_vault_l(f; run::AbstractString="phase1")
    outdir = mktempdir()
    try
        v = DataVault.Vault(FIXTURE_CFG_L; run=run, outdir=outdir)
        f(v, outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

allk(v) = ParamIO.expand(v.spec)

@testset "run! + keylock: each key processed exactly once across masters" begin
    with_vault_l() do v, outdir
        keys = allk(v)
        # Per-key counter: how many times work_fn was invoked for this key.
        # The lock guarantees "at most one invocation at a time per key" and
        # the is_done re-check ensures no duplicate invocation across masters.
        calls = Dict{String,Threads.Atomic{Int}}()
        for k in keys
            calls[canonical(k)] = Threads.Atomic{Int}(0)
        end
        work_fn = k -> begin
            Threads.atomic_add!(calls[canonical(k)], 1)
            sleep(0.01)
            return Dict{String,Any}("x" => 1)
        end

        # 4 masters racing on the same vault
        tasks = [
            Threads.@spawn(run!(work_fn, v, keys; opts=RunOpts(heartbeat_interval=10.0)))
            for _ in 1:4
        ]
        foreach(wait, tasks)

        for k in keys
            @test DataVault.is_done(v, k)
            # work_fn called exactly once per key, regardless of 4 masters
            @test calls[canonical(k)][] == 1
        end
    end
end

@testset "run! + keylock: :lock_busy event appears under contention" begin
    with_vault_l() do v, outdir
        keys = allk(v)
        work_fn = k -> (sleep(0.02); Dict{String,Any}("x" => 1))
        # :lock_busy is a :debug event — opt into it via log_level=:debug to assert it.
        tasks = [
            Threads.@spawn(
                run!(work_fn, v, keys; opts=RunOpts(; heartbeat_interval=10.0, log_level=:debug))
            ) for _ in 1:4
        ]
        foreach(wait, tasks)

        lines = _read_event_lines_l(outdir)
        kinds = [JSON3.read(l).kind for l in lines]
        # Either some locks were busy, or one master finished everything so
        # fast the others saw :skip_complete. Both are acceptable outcomes.
        @test ("lock_busy" in kinds) || ("skip_complete" in kinds)
        # And all keys are done
        for k in keys
            @test DataVault.is_done(v, k)
        end
    end
end

@testset "run! + retry: eventual success" begin
    with_vault_l() do v, outdir
        keys = allk(v)[1:1]
        attempts = Ref(0)
        work_fn = k -> begin
            attempts[] += 1
            if attempts[] < 3
                error("transient")
            end
            return Dict{String,Any}("x" => attempts[])
        end
        result = run!(work_fn, v, keys; opts=RunOpts(max_attempts=5))
        @test result.done == 1
        @test result.err == 0
        @test attempts[] == 3
        @test DataVault.is_done(v, keys[1])

        lines = _read_event_lines_l(outdir)
        kinds = [JSON3.read(l).kind for l in lines]
        @test "retry" in kinds
        @test !("gave_up" in kinds)
    end
end

@testset "run! + retry: gave_up after max_attempts" begin
    with_vault_l() do v, outdir
        keys = allk(v)[1:1]
        attempts = Ref(0)
        work_fn = k -> begin
            attempts[] += 1
            error("permanent")
        end
        result = run!(work_fn, v, keys; opts=RunOpts(max_attempts=3))
        @test attempts[] == 3
        @test result.gave_up == 1
        @test !DataVault.is_done(v, keys[1])

        # Manifest does NOT include this key
        m = load_manifest(v)
        @test !is_complete(m, keys[1])

        lines = _read_event_lines_l(outdir)
        kinds = [JSON3.read(l).kind for l in lines]
        @test "gave_up" in kinds
    end
end

@testset "run! + retry: re-run picks up gave_up key" begin
    with_vault_l() do v, outdir
        keys = allk(v)[1:1]
        # First run: permanent failure
        run!(k -> error("x"), v, keys; opts=RunOpts(max_attempts=2))
        @test !DataVault.is_done(v, keys[1])
        # .running should be cleaned up by try-finally, not orphaned
        @test !DataVault.is_running(v, keys[1])

        # Second run with working work_fn: should process the key
        counter = Ref(0)
        run!(
            k -> (counter[] += 1; Dict{String,Any}("x" => 1)),
            v,
            keys;
            opts=RunOpts(max_attempts=2),
        )
        @test counter[] == 1
        @test DataVault.is_done(v, keys[1])
    end
end

@testset "run! + retry: .running never orphaned on error" begin
    with_vault_l() do v, outdir
        keys = allk(v)
        # All keys fail permanently
        run!(k -> error("boom"), v, keys; opts=RunOpts(max_attempts=2))
        for k in keys
            @test !DataVault.is_running(v, k)
        end
    end
end
