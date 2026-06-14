# test_run_kill_recovery.jl — paths stay clean after a killed job
#
# These tests exercise the recovery path that kicks in on the NEXT
# `run!` invocation after a previous master was SIGKILL'd or
# SIGTERM'd mid-execution.  Post-v0.3 (KeyLock removed), the only
# on-disk artefact left behind by a crashed master is
#
#   status/<project>/<run>/<param_path>/sample_<NNN>.running
#
# which is exactly the atomic-acquire file managed by
# `DataVault.acquire_running!`.  No separate `locks/` tree exists any
# more.
#
# Desired behaviour:
#
# 1. `DataVault.cleanup_stale(vault)` removes any `.running` file whose
#    heartbeat is older than `stale_after`.  After cleanup, a fresh
#    `run!` completes the key normally, no `.running` leftover, a
#    `.done` file is written.
# 2. A live `run!` that crashes internally does not leak a stale
#    `.running` — the `finally` block in `_run_one_with_lock!` clears
#    it so siblings can retry immediately.
# 3. Even *without* calling `cleanup_stale` explicitly, a fresh `run!`
#    finds stale `.running` files and reclaims them via
#    `acquire_running!`'s built-in stale-reclaim path.
# 4. PM no longer creates any `locks/` directory — confirming the
#    "single source of truth for in-flight state" invariant on disk.

using ParallelManager, Test, DataVault, ParamIO, Dates

const FIXTURE_CFG_K = joinpath(@__DIR__, "fixtures", "study.toml")

function with_vault_k(f; run::AbstractString="kill_recovery")
    outdir = mktempdir()
    try
        v = DataVault.Vault(FIXTURE_CFG_K; run=run, outdir=outdir)
        f(v, outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

allk_k(v) = ParamIO.expand(v.spec)

"Fabricate a stale `.running` file whose heartbeat is `age_secs` old."
function _fabricate_stale_running!(
    vault::DataVault.Vault, key::DataVault.DataKey; age_secs::Real=3600
)
    DataVault.mark_running!(vault, key)
    path = DataVault._running_file(vault, key)
    old_dt = Dates.now() - Dates.Second(round(Int, age_secs))
    open(path, "w") do io
        println(io, "pid=", getpid())
        println(io, "started=", Dates.format(old_dt, "yyyy-mm-ddTHH:MM:SS"))
        return println(io, "heartbeat=", Dates.format(old_dt, "yyyy-mm-ddTHH:MM:SS"))
    end
    return path
end

# ── 1. No separate locks/ tree exists any more ─────────────────────────
@testset "post-v0.3: no locks/ directory is created" begin
    with_vault_k() do v, outdir
        keys = allk_k(v)
        work_fn = k -> Dict{String,Any}("sample" => k.sample)
        run!(work_fn, v, keys)

        @test !isdir(joinpath(outdir, "locks"))
        # status/ remains the single source of truth for in-flight state.
        @test isdir(joinpath(outdir, "status"))
    end
end

# ── 2. Explicit cleanup_stale + rerun recovers killed-master keys ──────
@testset "cleanup_stale + run! recovers a stale .running" begin
    with_vault_k() do v, outdir
        keys = allk_k(v)
        target = first(keys)

        # Simulate a master that marked this key running and then got
        # SIGKILL'd with a long-dead heartbeat.
        stale_running = _fabricate_stale_running!(v, target; age_secs=3600)
        @test isfile(stale_running)
        @test !DataVault.is_done(v, target)

        # cleanup_stale should reclaim it (age > stale_after).
        n = DataVault.cleanup_stale(v; stale_after=600)
        @test n >= 1
        @test !isfile(stale_running)

        # Fresh run! executes the key normally.
        work_fn = k -> Dict{String,Any}("sample" => k.sample)
        result = run!(work_fn, v, keys)
        @test result.done == length(keys)
        for k in keys
            @test DataVault.is_done(v, k)
            @test !isfile(DataVault._running_file(v, k))
        end
    end
end

# ── 3. acquire_running! reclaims stale .running inside run! itself ─────
#     (recovery without an explicit cleanup_stale call)
@testset "run! reclaims stale .running via acquire_running!" begin
    with_vault_k() do v, outdir
        keys = allk_k(v)
        target = first(keys)

        # Stale .running for every key, simulating a whole killed run.
        for k in keys
            _fabricate_stale_running!(v, k; age_secs=7200)
        end

        work_fn = k -> Dict{String,Any}("sample" => k.sample)
        opts = ParallelManager.RunOpts(; stale_after=60.0, heartbeat_interval=5.0)
        result = run!(work_fn, v, keys; opts=opts)

        @test result.done == length(keys)
        for k in keys
            @test DataVault.is_done(v, k)
            @test !isfile(DataVault._running_file(v, k))
        end
    end
end

# ── 4. work_fn exception does not leak a fresh .running ────────────────
@testset "run!: work_fn failure releases .running so siblings can retry" begin
    with_vault_k() do v, outdir
        keys = allk_k(v)
        target = first(keys)

        # A work_fn that always raises for the first key.
        counter = Ref(0)
        work_fn = k -> begin
            counter[] += 1
            k == target && error("boom")
            return Dict{String,Any}("sample" => k.sample)
        end

        opts = ParallelManager.RunOpts(; max_attempts=1)
        result = run!(work_fn, v, keys; opts=opts)

        # The failed key has no .done and no leftover .running.
        @test !DataVault.is_done(v, target)
        @test !isfile(DataVault._running_file(v, target))

        # Other keys completed normally.
        for k in keys
            k == target && continue
            @test DataVault.is_done(v, k)
            @test !isfile(DataVault._running_file(v, k))
        end
    end
end

# ── 5. Bulk stress: every key has a stale .running, all recover ────────
@testset "bulk: every key with stale .running recovers via cleanup + run!" begin
    with_vault_k() do v, outdir
        keys = allk_k(v)
        for k in keys
            _fabricate_stale_running!(v, k; age_secs=7200)
        end

        DataVault.cleanup_stale(v; stale_after=600)

        work_fn = k -> Dict{String,Any}("sample" => k.sample)
        result = run!(work_fn, v, keys)

        @test result.done == length(keys)
        @test !isdir(joinpath(outdir, "locks"))
        for k in keys
            @test DataVault.is_done(v, k)
            @test !isfile(DataVault._running_file(v, k))
        end
    end
end
