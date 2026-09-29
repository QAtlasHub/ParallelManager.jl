# A master whose work_fn never yields keeps its key.
#
# The key heartbeat used to be a task in the master. Under a work_fn that does not yield — a long
# BLAS call, a tight loop — it never beat, with -t 1, -t 2 and -t 2,1 alike; a sibling reclaimed
# the live key after `stale_after`, and the master committed it as well. It is now DataVault's
# heartbeat child. The master here runs in its own process (a busy loop in THIS process would
# stop the sibling too) and the sibling polls from this one.

using SweepRunner, Test, DataVault, ParamIO

const _BH_CFG = joinpath(@__DIR__, "fixtures", "study.toml")

function _bh_master_script(outdir, ready, busy_s)
    return """
    using SweepRunner, DataVault, ParamIO
    v = DataVault.Vault($(repr(_BH_CFG)); run="busy", outdir=$(repr(outdir)))
    k = DataVault.keys(v)[1]
    work = key -> begin
        touch($(repr(ready)))
        s = let t = time() + $(busy_s), x = 0.0
            while time() < t; x += sin(x) + 1e-9; end    # never yields
            x
        end
        Dict{String,Any}("x" => s)
    end
    r = run!(work, v, [k]; opts=RunOpts(workers=:sequential, stale_after=1.0,
                                         heartbeat_interval=0.2))
    print(r.done, " ", time())
    """
end

@testset "run!: a work_fn that never yields keeps its key against a sibling" begin
    for threads in (1, 2)
        outdir = mktempdir()
        try
            ready = joinpath(outdir, "ready")
            out = IOBuffer()
            exe = Base.julia_cmd()
            script = _bh_master_script(outdir, ready, 4.0)
            cmd = `$exe --startup-file=no -t $threads --project=$(Base.active_project()) -e $script`
            p = run(pipeline(cmd; stdout=out, stderr=devnull); wait=false)
            t0 = time()
            while !isfile(ready) && process_running(p) && time() - t0 < 180
                sleep(0.05)
            end
            @test isfile(ready)

            v = DataVault.Vault(_BH_CFG; run="busy", outdir=outdir)
            k = DataVault.keys(v)[1]
            t_ready = mtime(ready)                      # when work_fn began, not when we noticed
            got_at = Float64[]
            tries = 0
            while process_running(p) && !DataVault.is_done(v, k)
                tok = DataVault.new_owner_token()
                r = DataVault.acquire_running!(v, k, tok; stale_after=1.0)
                tries += 1
                if r !== :busy
                    push!(got_at, time())
                    DataVault.clear_running!(v, k, tok)
                end
                sleep(0.1)
            end
            wait(p)
            done, t_end = split(String(take!(out)))

            # The master really computed for several stale_after while the sibling tried.
            @test time() - t_ready >= 3.5
            @test tries >= 25
            # The sibling never got the key while the master held it (only after `run!` ended).
            @test count(<(parse(Float64, t_end)), got_at) == 0
            @test done == "1"
            @test DataVault.is_done(v, k)
        finally
            rm(outdir; recursive=true, force=true)
        end
    end
end

@testset "run!: a master whose key was reclaimed does not commit it" begin
    # The loss is made to happen mid-work_fn: the sibling reclaims with stale_after=0. The master
    # must return the key as `:lock_busy`, commit nothing, and leave the sibling's lock alone.
    outdir = mktempdir()
    try
        v = DataVault.Vault(_BH_CFG; run="lost", outdir=outdir)
        k = DataVault.keys(v)[1]
        sib = DataVault.new_owner_token()
        work =
            key -> begin
                @assert DataVault.acquire_running!(v, key, sib; stale_after=0.0) === :reclaimed
                Dict{String,Any}("x" => 1)
            end
        r = run!(work, v, [k]; opts=RunOpts(workers=:sequential))
        @test r.done == 0
        @test !DataVault.is_done(v, k)
        @test DataVault.running_owner(v, k) == sib
    finally
        rm(outdir; recursive=true, force=true)
    end
end
