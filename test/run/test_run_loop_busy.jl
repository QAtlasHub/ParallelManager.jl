# run_loop! and keys held by a sibling (#..): a round that completes nothing but finds `:lock_busy`
# is not an empty round until `stale_after` has been waited out.
#
# The bug this pins: `max_empty_rounds * idle_sleep` is 90 s by default and `stale_after` is 600 s,
# so a job following one the wall clock killed returned 8.5 minutes before the dead job's locks
# became reclaimable, left the campaign short, and reported nothing.

using SweepRunner, Test, DataVault, ParamIO

const _BUSY_CFG = joinpath(@__DIR__, "fixtures", "study.toml")

# Fast constants so the wait is seconds. `heartbeat_interval` must stay under `stale_after`.
function _opts(; kw...)
    return RunOpts(; workers=:sequential, stale_after=3.0, heartbeat_interval=1.0, kw...)
end
const _BUDGET = 3.0 + 2 * 0.5   # opts.stale_after + 2 * idle_sleep

function with_tail(f)
    outdir = mktempdir()
    try
        v = DataVault.Vault(_BUSY_CFG; run="tail", outdir=outdir)
        ks = DataVault.keys(v)
        for k in ks[2:end]                      # everything done but the first
            DataVault.save!(v, k, Dict("x" => 1))
            DataVault.mark_done!(v, k)
        end
        f(v, ks)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

@testset "run_loop!: a dead holder's key is waited out and completed" begin
    with_tail() do v, ks
        DataVault.mark_running!(v, ks[1])       # killed mid-key: the marker outlived the process
        n = Ref(0)
        t0 = time()
        r = run_loop!(
            k -> (n[] += 1; Dict{String,Any}("x" => 1)),
            v,
            ks;
            opts=_opts(),
            max_empty_rounds=2,
            idle_sleep=0.5,
        )
        elapsed = time() - t0

        @test n[] == 1                          # it ran the key
        @test DataVault.is_done(v, ks[1])       # and the campaign is complete
        @test r.done == 1
        # It waited stale_after out rather than giving up (the bug gave up after two empty rounds,
        # ~1 s). Less one second: DataVault writes `heartbeat=` truncated to the second, so the lock
        # reads up to 1 s older than it is and is reclaimed up to 1 s before stale_after. Asserting
        # the full 3.0 failed on hosted runners at 2.85 s and 2.90 s.
        @test elapsed >= 3.0 - 1.0
    end
end

@testset "run_loop!: with nothing held, it still exits promptly" begin
    # The control. Without this the fix would read as "always wait stale_after", which would make
    # every finished campaign pay 600 s to notice it is finished.
    with_tail() do v, ks
        DataVault.save!(v, ks[1], Dict("x" => 1))
        DataVault.mark_done!(v, ks[1])          # everything done, nothing running

        t0 = time()
        r = run_loop!(
            k -> Dict{String,Any}("x" => 1),
            v,
            ks;
            opts=_opts(),
            max_empty_rounds=2,
            idle_sleep=0.5,
        )
        elapsed = time() - t0

        @test r.done == 0
        @test r.busy == 0
        @test elapsed < 3.0                     # well under stale_after
    end
end

@testset "run_loop!: a LIVE sibling is not waited on forever" begin
    # The other side of the bound. A holder that keeps refreshing is alive and doing the work, so
    # this master has nothing to contribute and must return rather than spin.
    with_tail() do v, ks
        DataVault.mark_running!(v, ks[1])
        alive = Threads.Atomic{Bool}(true)
        beater = Threads.@spawn while alive[]
            DataVault.touch_running!(v, ks[1])  # the sibling's heartbeat, never going stale
            sleep(0.3)
        end
        try
            n = Ref(0)
            t0 = time()
            r = run_loop!(
                k -> (n[] += 1; Dict{String,Any}("x" => 1)),
                v,
                ks;
                opts=_opts(),
                max_empty_rounds=2,
                idle_sleep=0.5,
            )
            elapsed = time() - t0

            @test n[] == 0                      # never stole the live sibling's key
            @test !DataVault.is_done(v, ks[1])
            @test r.busy > 0                    # and SAYS it left work held, which it could not before
            @test elapsed >= _BUDGET            # waited the budget
            @test elapsed < _BUDGET + 10        # then returned
        finally
            alive[] = false
            wait(beater)
        end
    end
end
