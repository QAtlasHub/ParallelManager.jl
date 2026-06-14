# ─────────────────────────────────────────────────────────────────────────────
# Adversarial tests for run! / run_loop! / KeyLock / Manifest.
#
# These probe failure modes the happy-path tests miss:
#  - stop_flag written DURING pmap fan-out (not only before run!)
#  - KeyLock stale reclaim when holder dies (simulated by stale mtime)
#  - Corrupt manifest.jld2 → empty manifest, no crash
#  - work_fn that returns non-Dict / throws after partial completion
#  - run! over a vault where some keys already have `.running` orphans
#    (exactly the situation the try-finally fix in #8 created a regression for)
# ─────────────────────────────────────────────────────────────────────────────

using ParallelManager, Test, DataVault, ParamIO, JSON3, JLD2
using Distributed
using Dates

const FIXTURE_CFG_A = joinpath(@__DIR__, "fixtures", "study.toml")

function with_vault_a(f; run::AbstractString="phase1")
    outdir = mktempdir()
    try
        v = DataVault.Vault(FIXTURE_CFG_A; run=run, outdir=outdir)
        f(v, outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

allk_a(v) = ParamIO.expand(v.spec)

# ═════════════════════════════════════════════════════════════════════════════
# 1. stop_flag written mid-run by a concurrent task
# ═════════════════════════════════════════════════════════════════════════════

@testset "run!: stop_flag raised mid-execution by concurrent task" begin
    with_vault_a() do v, outdir
        keys = allk_a(v)
        @test length(keys) >= 4

        stop = joinpath(outdir, "STOP_NOW")
        # Delay the stop flag by 50 ms so a few keys finish first
        t = Timer(_ -> touch(stop), 0.05)

        # work_fn sleeps so we're sure the stop is seen after some work
        counter = Ref(0)
        work_fn = k -> begin
            counter[] += 1
            sleep(0.02)
            Dict{String,Any}("x" => 1)
        end

        result = run!(work_fn, v, keys; opts=RunOpts(stop_flag=stop))
        close(t)

        # At least one key ran before stop kicked in, at least one was skipped
        @test result.done >= 1
        @test result.done < length(keys)  # stop really cut us short
        # Remaining keys are either counted as :stop (if we reached their
        # dispatch) or just dropped from the todo list (sequential loop
        # break). Either way, fewer done+stop than length means some were
        # silently un-processed.
        @test counter[] == result.done  # work_fn only called for done keys
        return isfile(stop) && rm(stop; force=true)
    end
end

# ═════════════════════════════════════════════════════════════════════════════
# 2. .running orphans from crashed prior runs are resumed cleanly
# ═════════════════════════════════════════════════════════════════════════════

@testset "run!: stale .running orphan is overwritten and completed" begin
    with_vault_a() do v, outdir
        keys = allk_a(v)
        target = keys[1]

        # Simulate a crashed prior run with an OLD heartbeat: `.running`
        # exists but its `heartbeat=` line predates any sane stale_after
        # threshold.  Post-v0.3 PM delegates locking to
        # `DataVault.acquire_running!`, which treats a fresh `.running`
        # as `:busy` (correct) and a stale one as reclaimable —
        # exactly the crashed-run recovery path.
        DataVault.mark_running!(v, target)
        running_path = DataVault._running_file(v, target)
        old_str = Dates.format(Dates.now() - Dates.Second(7200), "yyyy-mm-ddTHH:MM:SS")
        lines = readlines(running_path)
        open(running_path, "w") do io
            for line in lines
                println(io, startswith(line, "heartbeat=") ? "heartbeat=$old_str" : line)
            end
        end
        @test DataVault.is_running(v, target)
        @test !DataVault.is_done(v, target)
        @test DataVault.running_age_secs(v, target) > 600

        # New run with a modest stale_after reclaims the stale .running
        # via acquire_running! and completes the key.
        work_fn = k -> Dict{String,Any}("x" => 1)
        opts = RunOpts(; stale_after=600.0)
        result = run!(work_fn, v, keys; opts=opts)
        @test result.done == length(keys)
        @test DataVault.is_done(v, target)
        # .running must be cleaned up
        @test !DataVault.is_running(v, target)
    end
end

# ═════════════════════════════════════════════════════════════════════════════
# 3. Corrupt manifest.jld2 → treated as empty, run completes normally
# ═════════════════════════════════════════════════════════════════════════════

@testset "run!: corrupt manifest.jld2 is recoverable" begin
    with_vault_a() do v, outdir
        keys = allk_a(v)
        # Write a garbage file where the manifest would be
        mpath = manifest_path(manifest_root(v), Symbol(v.run))
        mkpath(dirname(mpath))
        write(mpath, "this is not a JLD2 file")
        @test isfile(mpath)

        # run! should not crash; load_manifest returns empty and runs all keys
        work_fn = k -> Dict{String,Any}("x" => 1)
        result = run!(work_fn, v, keys)
        @test result.done == length(keys)
        # After run, the corrupt file is overwritten with a valid manifest
        m = load_manifest(v)
        @test length(m.complete) == length(keys)
    end
end

# ═════════════════════════════════════════════════════════════════════════════
# 4. KeyLock: stale reclaim when holder died before first heartbeat
# ═════════════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════════════════════
# 4. Atomic .running reclaim: DataVault.acquire_running! is the lock primitive
# ═════════════════════════════════════════════════════════════════════════════
#
# Post-v0.3 PM delegates locking entirely to `DataVault.acquire_running!`
# (POSIX link()).  The equivalent "stale holder that crashed before first
# heartbeat" case is now covered in `test_run_kill_recovery.jl` and in
# DataVault's own test/vault/test_vault.jl "acquire_running!" testsets,
# so the KeyLock-specific legacy case here has been removed.

# ═════════════════════════════════════════════════════════════════════════════
# 5. Multi-task race with VERY short heartbeat interval
#    (stress test: ensures heartbeat loop doesn't deadlock with release)
# ═════════════════════════════════════════════════════════════════════════════

@testset "run!: 8-master race with fast heartbeat (stress)" begin
    with_vault_a() do v, outdir
        keys = allk_a(v)
        invoked = Dict{String,Threads.Atomic{Int}}()
        for k in keys
            invoked[canonical(k)] = Threads.Atomic{Int}(0)
        end
        work_fn = k -> begin
            Threads.atomic_add!(invoked[canonical(k)], 1)
            sleep(0.01)
            Dict{String,Any}("x" => 1)
        end

        # 8 concurrent threads, very aggressive heartbeat
        tasks = [
            Threads.@spawn(
                run!(
                    work_fn, v, keys; opts=RunOpts(heartbeat_interval=0.05, stale_after=1.0)
                )
            ) for _ in 1:8
        ]
        foreach(wait, tasks)

        for k in keys
            @test DataVault.is_done(v, k)
            # Under contention, each key should be invoked at most a few times
            # (ideally exactly once, but re-runs inside stale reclaim windows
            # are acceptable)
            @test invoked[canonical(k)][] >= 1
            @test invoked[canonical(k)][] <= 3
        end
    end
end

# ═════════════════════════════════════════════════════════════════════════════
# 6. merge_and_save_manifest!: two concurrent masters don't lose keys
# ═════════════════════════════════════════════════════════════════════════════

@testset "merge_and_save_manifest!: preserves keys across concurrent writers" begin
    with_vault_a() do v, outdir
        # Pre-create two disjoint manifests on different in-memory objects
        # but same on-disk location, simulate race where each master
        # starts with its own completed set.
        ka = ParamIO.DataKey(Dict{String,Any}("N" => 4, "J" => 0.5), 0)
        kb = ParamIO.DataKey(Dict{String,Any}("N" => 4, "J" => 1.0), 0)
        kc = ParamIO.DataKey(Dict{String,Any}("N" => 8, "J" => 0.5), 0)

        root = manifest_root(v)
        stage = Symbol(v.run)

        m_A = Manifest(stage, root)
        add_complete!(m_A, ka)
        add_complete!(m_A, kb)
        # Save A's state first (plain save, like an older master might have done)
        save_manifest(m_A)

        # Master B has only {c} in memory; merges should give {a,b,c}
        m_B = Manifest(stage, root)
        add_complete!(m_B, kc)
        merge_and_save_manifest!(m_B)

        final = load_manifest(root, stage)
        @test is_complete(final, ka)
        @test is_complete(final, kb)
        @test is_complete(final, kc)
    end
end

# ═════════════════════════════════════════════════════════════════════════════
# 7. run! doesn't crash when work_fn throws on EVERY key
# ═════════════════════════════════════════════════════════════════════════════

@testset "run!: all keys fail → no crash, all .running cleaned" begin
    with_vault_a() do v, outdir
        keys = allk_a(v)
        result = run!(k -> error("always fails"), v, keys; opts=RunOpts(max_attempts=2))
        @test result.done == 0
        @test result.gave_up == length(keys)
        # No .running orphans
        for k in keys
            @test !DataVault.is_running(v, k)
            @test !DataVault.is_done(v, k)
        end
    end
end
