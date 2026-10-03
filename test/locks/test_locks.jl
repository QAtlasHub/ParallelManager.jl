# Locks (#68): whether a `.running` is real is asked of the holder's master, and a job that starts
# reconciles the locks it finds.

using SweepRunner, Test, DataVault, ParamIO, JSON3
using SweepRunner: locks, lock_summary, next_task!, start_task!

const _LK_CFG = joinpath(@__DIR__, "..", "run", "fixtures", "study.toml")

function _lk_vault(f; run="lk")
    outdir = mktempdir()
    try
        f(DataVault.Vault(_LK_CFG; run=run, outdir=outdir), outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

function _lk_events(outdir)
    logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir(outdir))
    return [JSON3.read(l) for f in logs for l in readlines(joinpath(outdir, f))]
end

# A master's status as `read_status` returns it. The hosts are not this one, so the scheduler /
# pid fallback has no opinion and only the master's answer decides.
function _lk_master(;
    held=String[], workers=[("nodeA", 4242)], state="running", stale=false, updated=time()
)
    return Dict{String,Any}(
        "master" => "login9_1",
        "host" => "login9",
        "pid" => 1,
        "state" => state,
        "stale" => stale,
        "updated" => updated,
        "interval" => 60.0,
        "held" => held,
        "worker_table" => [
            Dict{String,Any}("host" => h, "pid" => p, "id" => i + 1) for
            (i, (h, p)) in enumerate(workers)
        ],
    )
end

# The same, written where `read_status(vault)` finds it.
function _lk_write_master(v; kwargs...)
    m = _lk_master(; kwargs...)
    dir = joinpath(state_root(v), "masters", m["master"])
    mkpath(dir)
    d = copy(m)
    delete!(d, "stale")
    d["stage"] = v.run
    d["started"] = time() - 100
    d["job"] = ""
    write(joinpath(dir, "status.json"), JSON3.write(d))
    return m
end

const _LK_TOK = "nodeA:4242:0badc0de"

@testset "judge_lock: only a master that can be asked makes a lock dead" begin
    j(owner, age, masters; kw...) = first(judge_lock(owner, age, masters; kw...))

    # Nobody to ask: the heartbeat's age is all there is.
    @test j(nothing, 5.0, []) === :unknown
    @test j(nothing, 700.0, []) === :stale
    @test j(_LK_TOK, 5.0, []) === :unknown
    @test j(_LK_TOK, 700.0, []) === :stale
    @test j(_LK_TOK, 30.0, []; stale_after=10.0) === :stale

    # The master lists it: held, however old the heartbeat.
    @test j(_LK_TOK, 5.0, [_lk_master(; held=[_LK_TOK])]) === :held
    @test j(_LK_TOK, 9000.0, [_lk_master(; held=[_LK_TOK])]) === :held

    # The master knows the holder process, reported after the lock's last heartbeat, and does
    # not list it: dead, at once, without waiting for stale_after.
    v, why, who = judge_lock(_LK_TOK, 30.0, [_lk_master()])
    @test v === :dead
    @test who == "login9_1"
    @test occursin("nodeA:4242", why)
    @test j(_LK_TOK, 30.0, [_lk_master(; state="ended")]) === :dead

    # ...but not when its report is OLDER than the heartbeat: the lock may have been taken since.
    @test j(_LK_TOK, 30.0, [_lk_master(; updated=time() - 60)]) === :unknown
    # ...nor when the report and the heartbeat are within the clock-skew margin of each other.
    @test j(_LK_TOK, 2.0, [_lk_master()]) === :unknown
    # A master that does not know that process has nothing to say about it.
    @test j(_LK_TOK, 30.0, [_lk_master(; workers=[("nodeB", 4242)])]) === :unknown
    # A master that went silent is not asked.
    @test j(_LK_TOK, 30.0, [_lk_master(; stale=true)]) === :unknown
    @test j(_LK_TOK, 30.0, [_lk_master(; stale=true, held=[_LK_TOK])]) === :unknown
    # A status written before masters listed their locks is not an answer either.
    old = _lk_master()
    delete!(old, "held")
    @test j(_LK_TOK, 30.0, [old]) === :unknown
    # The master process itself is "known" too (the sequential case).
    @test j("login9:1:0badc0de", 30.0, [_lk_master()]) === :dead
end

@testset "judge_lock: a lock this process has out is held, whatever a status says" begin
    _lk_vault() do v, _
        k = DataVault.keys(v)[1]
        SweepRunner._out_add!(_LK_TOK, v, k)
        try
            @test first(judge_lock(_LK_TOK, 30.0, [_lk_master()])) === :held
        finally
            SweepRunner._out_remove!(_LK_TOK)
        end
        @test first(judge_lock(_LK_TOK, 30.0, [_lk_master()])) === :dead
    end
end

@testset "run!: an orphan its master disowns is reaped before the queue is built" begin
    # stale_after is ten minutes and the heartbeat is fresh: only the master's answer can free
    # this key inside the test.
    _lk_vault() do v, outdir
        ks = DataVault.keys(v)
        @test DataVault.acquire_running!(v, ks[1], _LK_TOK) === :ok
        _lk_write_master(v; updated=time() + 10)      # reported after the lock's heartbeat
        r = run!(k -> Dict{String,Any}("x" => 1), v, ks; opts=RunOpts(; stale_after=600.0))
        @test r.done == length(ks)
        @test r.busy == 0
        ev = _lk_events(outdir)
        reaped = only([e for e in ev if e.kind == "lock_reaped"])
        @test reaped.owner == _LK_TOK
        @test occursin("login9_1", reaped.why)
        rec = only([e for e in ev if e.kind == "locks_reconciled"])
        @test (rec.locks, rec.reaped, rec.held, rec.unknown) == (1, 1, 0, 0)
        # ...and the job's own status carries the same totals.
        mine = only([s for s in read_status(v) if s["master"] != "login9_1"])
        @test mine["locks"]["reaped"] == 1
    end
end

@testset "run!: a lock its master still has out is left alone, past stale_after too" begin
    _lk_vault() do v, outdir
        ks = DataVault.keys(v)
        @test DataVault.acquire_running!(v, ks[1], _LK_TOK) === :ok
        _lk_write_master(v; held=[_LK_TOK], updated=time() + 10)
        sleep(1.2)                                           # the heartbeat is now "stale"
        ran = Ref(0)
        r = run!(
            k -> (ran[] += 1; Dict{String,Any}("x" => 1)),
            v,
            ks;
            opts=RunOpts(; stale_after=1.0, heartbeat_interval=0.5),
        )
        @test r.done == length(ks) - 1
        @test r.busy == 1
        @test ran[] == length(ks) - 1
        @test DataVault.running_owner(v, ks[1]) == _LK_TOK
        rec = [e for e in _lk_events(outdir) if e.kind == "locks_reconciled"]
        @test rec[1].held == 1
        @test rec[1].reaped == 0
    end
end

@testset "run!: while a key runs, its lock is in the list a sibling would ask" begin
    _lk_vault() do v, _
        ks = DataVault.keys(v)
        seen = Bool[]
        work = k -> begin
            tok = DataVault.running_owner(v, k)
            push!(seen, tok in SweepRunner._out_tokens())
            push!(seen, first(judge_lock(tok, 0.0, [])) === :held)
            return Dict{String,Any}("x" => 1)
        end
        r = run!(work, v, ks)
        @test r.done == length(ks)
        @test all(seen) && length(seen) == 2 * length(ks)
        @test isempty(SweepRunner._out_tokens())
        @test isempty(only(read_status(v))["held"])
    end
end

@testset "locks: listing, by key, by vault and by outdir, and reaping only the dead" begin
    _lk_vault() do v, outdir
        ks = DataVault.keys(v)
        # One held by a process that is alive (this one), one by a pid that has exited.
        live = owner_token()
        @test DataVault.acquire_running!(v, ks[1], live) === :ok
        p = run(`sleep 0.01`; wait=false)
        gone_pid = getpid(p)
        gone = string(gethostname(), ":", gone_pid, ":0000abcd")
        wait(p)
        @test DataVault.acquire_running!(v, ks[2], gone) === :ok

        infos = locks(v, ks)
        @test length(infos) == 2
        by = Dict(l.key => l for l in infos)
        @test by[ParamIO.canonical(ks[1])].verdict === :held
        @test by[ParamIO.canonical(ks[2])].verdict === :dead
        @test by[ParamIO.canonical(ks[2])].pid == gone_pid
        s = lock_summary(infos)
        @test (s.held, s.dead, s.stale, s.unknown) == (1, 1, 0, 0)

        # Without keys the locks are found by walking, and named by path.
        @test sort([l.verdict for l in locks(v)]) == [:dead, :held]
        @test sort([l.verdict for l in locks(outdir)]) == [:dead, :held]
        out = sprint(io -> print_locks(io, outdir))
        @test startswith(out, "locks: 1 held by 1 job(s), 1 dead (1 dead job(s)), 0 stale")
        @test occursin("dead    ", out) && occursin("held    ", out)
        @test SweepRunner.cli(["locks", outdir]; io=IOBuffer()) == 0

        # Listing removed nothing.
        @test DataVault.is_running(v, ks[2])
        r = reap_dead_locks!(v, ks)
        @test (r.reaped, r.dead, r.held) == (1, 1, 1)
        @test !DataVault.is_running(v, ks[2])
        @test DataVault.running_owner(v, ks[1]) == live       # the live one is untouched
        DataVault.clear_running!(v, ks[1], live)
    end
end

@testset "reaping a whole outdir: dead locks on any key, found by path, and only those (#112)" begin
    _lk_vault() do v, outdir
        ks = DataVault.keys(v)
        live = owner_token()
        @test DataVault.acquire_running!(v, ks[1], live) === :ok
        gone = map(2:3) do i
            p = run(`sleep 0.01`; wait=false)
            tok = string(gethostname(), ":", getpid(p), ":0000abc", i)
            wait(p)
            @test DataVault.acquire_running!(v, ks[i], tok) === :ok
            tok
        end
        # No key is named: the form a shell uses on a vault whose runs it does not know.
        io = IOBuffer()
        @test SweepRunner.cli(["locks", outdir]; io=io) == 0
        @test all(i -> DataVault.is_running(v, ks[i]), 1:3)      # without --reap: nothing
        io = IOBuffer()
        @test SweepRunner.cli(["locks", outdir, "--reap"]; io=io) == 0
        @test occursin("reaped 2 of 2 dead lock(s)", String(take!(io)))
        @test !DataVault.is_running(v, ks[2]) && !DataVault.is_running(v, ks[3])
        @test DataVault.running_owner(v, ks[1]) == live           # held: untouched
        r = reap_dead_locks!(v)
        @test (r.reaped, r.dead, r.held, r.failed) == (0, 0, 1, 0)
        DataVault.clear_running!(v, ks[1], live)
    end
end

@testset "a master that leaves releases the locks it still has out, and says which" begin
    _lk_vault() do v, outdir
        ks = DataVault.keys(v)
        log = SweepRunner.EventLog(joinpath(outdir, "events_x.jsonl"))
        t = TaskTable(ks)
        tok = owner_token()
        i = next_task!(t, 1)
        start_task!(t, i, tok, 1)
        SweepRunner._out_add!(tok, v, ks[i])
        @test DataVault.acquire_running!(v, ks[i], tok) === :ok

        @test SweepRunner._release_running!(t, v, :lk, log) == 1
        @test !DataVault.is_running(v, ks[i])
        @test !(tok in SweepRunner._out_tokens())
        @test t.rows[i].outcome === :lock_busy                 # retriable, not failed
        rel = only([e for e in _lk_events(outdir) if e.kind == "lock_released"])
        @test rel.why == "master_exit"
        @test rel.key == ParamIO.canonical(ks[i])

        # The exit hook does the same for whatever is out when the process is told to stop.
        tok2 = owner_token()
        @test DataVault.acquire_running!(v, ks[2], tok2) === :ok
        SweepRunner._out_add!(tok2, v, ks[2])
        SweepRunner._release_all_at_exit()
        @test !DataVault.is_running(v, ks[2])
        @test isempty(SweepRunner._out_tokens())
    end
end

@testset "one reading of the allocation's cores, and of a key's lock (#116)" begin
    @test SweepRunner._slurm_cpus_per_node("128(x2),64") == [128, 128, 64]
    @test SweepRunner._slurm_cpus_per_node("") == Int[]
    @test SweepRunner._slurm_cpus_per_node("many") === nothing
    # The status takes what cannot be read as "not known"; the pool, which places by it, refuses.
    withenv("SLURM_JOB_CPUS_PER_NODE" => "128(x2),64") do
        @test SweepRunner._slurm_alloc_cores() == 320
    end
    withenv("SLURM_JOB_CPUS_PER_NODE" => "many") do
        @test SweepRunner._slurm_alloc_cores() == 0
    end
    @test_throws ErrorException SweepRunner._expand_slurm_counts("many", 1)
    _lk_vault() do v, _
        k = DataVault.keys(v)[1]
        @test SweepRunner._lock_now(v, k) === nothing
        tok = owner_token()
        @test DataVault.acquire_running!(v, k, tok) === :ok
        lk = SweepRunner._lock_now(v, k)
        @test lk.owner == tok && 0 <= lk.age < 60
        DataVault.clear_running!(v, k, tok)
        @test SweepRunner._lock_now(v, k) === nothing
    end
end

# A master as its own process, inside a key that does not end: what a scheduler's SIGTERM finds.
const _LK_TERM = raw"""
using SweepRunner, DataVault
cfg, outdir = ARGS
v = DataVault.Vault(cfg; run="term", outdir=outdir)
work = k -> begin
    touch(joinpath(outdir, "in_key"))
    while true
        sleep(0.05)
    end
end
run!(work, v, DataVault.keys(v)[1:1]; opts=RunOpts(; control_interval=0))
"""

@testset "a master sent SIGTERM releases its lock on the way out (#111)" begin
    _lk_vault(; run="term") do v, outdir
        k = DataVault.keys(v)[1]
        script = joinpath(outdir, "master.jl")
        write(script, _LK_TERM)
        julia = `$(Base.julia_cmd()) --startup-file=no --project=$(dirname(Base.active_project()))`
        err = joinpath(outdir, "err")
        p = run(pipeline(`$julia $script $_LK_CFG $outdir`; stderr=err); wait=false)
        t0 = time()
        while !isfile(joinpath(outdir, "in_key")) && process_running(p) && time() - t0 < 300
            sleep(0.1)
        end
        @test isfile(joinpath(outdir, "in_key"))
        @test DataVault.is_running(v, k)                        # it holds the key
        kill(p, Base.SIGTERM)
        t0 = time()
        while process_running(p) && time() - t0 < 60
            sleep(0.1)
        end
        @test !process_running(p)
        process_running(p) && kill(p, Base.SIGKILL)
        # The lock went with it: the next job does not wait `stale_after` for this key.
        @test !DataVault.is_running(v, k)
        @test !DataVault.is_done(v, k)
    end
end
