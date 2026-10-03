# Pool (#70, #79): workers sized to the keys they run, started where a node has the room.

using SweepRunner, Test, DataVault, ParamIO, JSON3, Distributed, LinearAlgebra
using SweepRunner: StepManager, shutdown!

const _PL_CFG = joinpath(@__DIR__, "..", "run", "fixtures", "study.toml")       # N in (4, 8)

function _pl_vault(f; run="pl")
    outdir = mktempdir()
    try
        f(DataVault.Vault(_PL_CFG; run=run, outdir=outdir), outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

function _pl_events(outdir)
    logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir(outdir))
    return [JSON3.read(l) for f in logs for l in readlines(joinpath(outdir, f))]
end

# A pool over "one node" of this machine, cleaned up whatever happens.
function _pl_pool(f; cores=4, mem_gb=8.0, kw...)
    nprocs() > 1 && rmprocs(workers())
    # `keep`: these tests look at the pool's workers after the run.
    pool = SizedPool(LocalSpawner(; cores, mem_gb); poll=0.2, keep=true, kw...)
    try
        f(pool)
    finally
        shutdown!(pool)
        nprocs() > 1 && rmprocs(workers())
        note_workers!(; planned=0, launched=0)
    end
end

_pl_node(c=127, m=223.0) = PoolNode("n1", c, m)

@testset "worker_size: threads follow the memory, or the policy" begin
    n = _pl_node()
    # 9 GB is the share of ~5 cores on a 127-core / 223 GB node: a 1-core worker would strand 4.
    @test worker_size(KeyReq(1, 9.0), n, 127, 223.0) == KeyReq(5, 9.0)
    @test worker_size(KeyReq(1, 1.0), n, 127, 223.0) == KeyReq(1, 1.0)
    @test worker_size(KeyReq(4, 9.0), n, 127, 223.0) == KeyReq(5, 9.0)
    @test worker_size(KeyReq(1, 100.0), n, 127, 223.0) == KeyReq(8, 100.0)     # max_threads
    @test worker_size(KeyReq(1, 100.0), n, 127, 223.0; max_threads=16) == KeyReq(16, 100.0)
    @test worker_size(KeyReq(12, 2.0), n, 127, 223.0) == KeyReq(12, 2.0)       # never fewer
    @test worker_size(KeyReq(1, 9.0), n, 2, 223.0) == KeyReq(1, 9.0)           # only 2 cores free
    @test worker_size(KeyReq(1, 9.0), n, 127, 0.0) == KeyReq(1, 9.0)
    # :fastest takes the threads, and the memory that comes with them.
    f = worker_size(KeyReq(2, 4.0), n, 127, 223.0; threads=:fastest)
    @test f.cores == 8 && f.mem_gb ≈ 8 * 223 / 127
    @test worker_size(KeyReq(2, 40.0), n, 3, 223.0; threads=:fastest) == KeyReq(3, 40.0)
    # A key that says it is serial stays serial under :fastest.
    @test worker_size(KeyReq(1, 1.0), n, 127, 223.0; threads=:fastest) == KeyReq(1, 1.0)
end

@testset "plan_spawns: cover, place, backfill, and stop backfilling for a starved key" begin
    nodes = [PoolNode("a", 4, 8.0), PoolNode("b", 4, 16.0)]
    free_c = Dict("a" => 4, "b" => 4)
    free_m = Dict("a" => 8.0, "b" => 16.0)
    small, big = KeyReq(1, 1.0), KeyReq(4, 12.0)

    # An idle or starting worker takes one need each; the rest are started.
    p = plan_spawns(fill(small, 3), zeros(3), nodes, free_c, free_m, [KeyReq(1, 2.0)])
    @test length(p.starts) == 2 && isempty(p.blocked)
    # On the node that keeps the most memory free.
    @test first(p.starts)[1] == "b"
    # The dictionaries are the caller's, untouched.
    @test free_c == Dict("a" => 4, "b" => 4)

    # The big key takes all of node b; a second big one fits nowhere.
    p = plan_spawns([big, big, small], zeros(3), nodes, free_c, free_m, KeyReq[])
    @test p.starts[1] == ("b", KeyReq(4, 12.0))
    @test p.blocked == [2]
    @test length(p.starts) == 2                       # the small one went past it (backfill)
    @test p.starts[2][1] == "a"

    # Once the blocked key has waited long enough, nothing is started ahead of it.
    p = plan_spawns(
        [big, big, small],
        [0.0, 700.0, 0.0],
        nodes,
        free_c,
        free_m,
        KeyReq[];
        starve_after=600,
    )
    @test length(p.starts) == 1 && p.blocked == [2]

    # No more than there is room for (the per-master worker limit).
    p = plan_spawns(fill(small, 6), zeros(6), nodes, free_c, free_m, KeyReq[]; room=2)
    @test length(p.starts) == 2
    @test p.capped                                    # more were wanted than there was room for
    @test !plan_spawns(fill(small, 2), zeros(2), nodes, free_c, free_m, KeyReq[]; room=2).capped
    # A node left out (drained) is not used, whatever room it has.
    p = plan_spawns([big], [0.0], nodes[1:1], free_c, free_m, KeyReq[])
    @test isempty(p.starts) && p.blocked == [1]
    # Cores run out before memory here: 8 one-core workers, not 24.
    p = plan_spawns(
        fill(small, 24), zeros(24), nodes, free_c, free_m, KeyReq[]; max_threads=1
    )
    @test length(p.starts) == 8
    @test length(p.blocked) == 16
end

@testset "SlurmStepSpawner: the nodes an allocation offers, and the step it starts" begin
    env = Dict(
        "SLURM_JOB_NODELIST" => "c[01-03]",
        "SLURM_JOB_CPUS_PER_NODE" => "128(x2),64",
        "SLURM_MEM_PER_CPU" => "1800",
    )
    ns = SweepRunner._slurm_pool_nodes(env, "c01"; master_gb=3.0, headroom_gb=1.0)
    @test [n.name for n in ns] == ["c01", "c02", "c03"]
    @test [n.cores for n in ns] == [127, 128, 64]                    # the master keeps a core
    @test ns[2].mem_gb ≈ 128 * 1800 / 1024 - 1.0
    @test ns[1].mem_gb ≈ 128 * 1800 / 1024 - 1.0 - 3.0
    # A master of a node group sees only its group.
    part = SweepRunner._slurm_pool_nodes(
        env, "c01"; only=Set(["c02", "c03"]), master_gb=3.0, headroom_gb=1.0
    )
    @test [n.name for n in part] == ["c02", "c03"]
    env2 = merge(env, Dict("SLURM_MEM_PER_NODE" => "200000"))
    @test SweepRunner._slurm_pool_nodes(env2, "x"; master_gb=3.0, headroom_gb=0.0)[3].mem_gb ≈
        200000 / 1024
    delete!(env, "SLURM_MEM_PER_CPU")
    @test_throws ErrorException SweepRunner._slurm_pool_nodes(
        env, "c01"; master_gb=3.0, headroom_gb=1.0
    )
    @test SweepRunner._expand_slurm_counts("128(x2),64", 3) == [128, 128, 64]
    @test_throws ErrorException SweepRunner._expand_slurm_counts("128(x2)", 3)

    cmd = SweepRunner._step_command(StepManager("c02", 4, 9.5, true, 1), `julia --worker`)
    @test cmd.exec[1:2] == ["srun", "--exact"]
    @test "--nodelist=c02" in cmd.exec && "--cpus-per-task=4" in cmd.exec
    @test "--mem=9728M" in cmd.exec
    @test cmd.exec[(end - 1):end] == ["julia", "--worker"]
    # Not under srun, the worker command is what runs.
    @test SweepRunner._step_command(StepManager("x", 1, 1.0, false, 1), `julia --worker`).exec ==
        ["julia", "--worker"]
    @test_throws ArgumentError SizedPool(
        LocalSpawner(); key_req=k -> KeyReq(1, 1.0), threads=:x
    )
end

@testset "measured_speedup, and the cores :finish_by asks for" begin
    cost(class, cores, wall) =
        KeyCost("s", "k", class, wall, wall, cores, 1, "h", 1, Dict{String,Any}())
    cs = [cost("a", 1, 100.0), cost("a", 1, 120.0), cost("a", 4, 50.0), cost("b", 2, 10.0)]
    sp = measured_speedup(cs, k -> k.params["c"])
    ka = ParamIO.DataKey(Dict{String,Any}("c" => "a"), 1)
    kb = ParamIO.DataKey(Dict{String,Any}("c" => "b"), 1)
    @test sp(ka, 1) == 1.0
    @test sp(ka, 4) == 100.0 / 50.0           # median at 1 core over median at 4
    @test sp(ka, 3) == 1.0                    # not measured above 1 core until 4: no claim
    @test sp(ka, 8) == 100.0 / 50.0
    @test sp(kb, 8) == 1.0                    # one thread count measured: no claim

    row = TaskTable([ka]).rows[1]
    mk(threads) = SizedPool(
        LocalSpawner(; cores=16, mem_gb=64.0);
        key_req=k -> KeyReq(1, 2.0),
        threads=threads,
        speedup=(k, c) -> Float64(c),
        max_threads=8,
    )
    need = SweepRunner._pool_need
    # 250 s of work at one core, 100 s left: three cores get it there.
    @test need(mk(:finish_by), row, time() + 100, k -> 250.0) == KeyReq(3, 2.0)
    @test need(mk(:finish_by), row, time() + 100, k -> 50.0) == KeyReq(1, 2.0)
    @test need(mk(:finish_by), row, time() + 1, k -> 1e6) == KeyReq(8, 2.0)     # all it may have
    @test need(mk(:finish_by), row, nothing, k -> 250.0) == KeyReq(1, 2.0)      # no deadline
    @test need(mk(:throughput), row, time() + 100, k -> 250.0) == KeyReq(1, 2.0)
end

@testset "a pool starts a worker of each size, and a worker only takes keys it can hold" begin
    _pl_pool(;
        key_req=k -> k.params["N"] == 8 ? KeyReq(2, 3.0) : KeyReq(1, 1.0), retire_after=0.5
    ) do pool
        _pl_vault() do v, outdir
            ks = DataVault.keys(v)
            @test nprocs() == 1                                   # nothing was started by hand
            work =
                k -> Dict{String,Any}(
                    "N" => k.params["N"],
                    "pid" => Distributed.myid(),
                    "threads" => LinearAlgebra.BLAS.get_num_threads(),
                )
            r = run!(work, v, ks; pool=pool, load=[:Distributed, :LinearAlgebra])
            @test r.done == length(ks)
            @test (r.err, r.busy) == (0, 0)
            for k in ks
                d = DataVault.load(v, k)
                # A key ran on a worker with the threads its size gives it.
                @test d["threads"] >= (k.params["N"] == 8 ? 2 : 1)
                @test d["pid"] != 1
            end
            spawned = [e for e in _pl_events(outdir) if e.kind == "pool_spawn"]
            @test Set((e.cores, e.mem_gb) for e in spawned) ⊇ Set([(1, 1.0), (2, 3.0)])
            @test sum(e.n for e in spawned) >= 2
            sizes = pool_summary(pool)
            @test !isempty(sizes) && all(s -> s.workers >= 1, sizes)
            # The room in use is what the workers hold.
            used = sum(w.size.cores for w in values(pool.workers))
            @test pool.free_c[gethostname()] == 4 - used
        end
    end
    @test nprocs() == 1                                           # shutdown! removed them
end

@testset "a key no node can hold is reported once, and the rest runs" begin
    _pl_pool(; key_req=k -> k.params["N"] == 8 ? KeyReq(64, 1.0) : KeyReq(1, 1.0)) do pool
        _pl_vault() do v, outdir
            ks = DataVault.keys(v)
            r = run!(k -> Dict{String,Any}("x" => 1), v, ks; pool=pool)
            nbig = count(k -> k.params["N"] == 8, ks)
            @test r.err == nbig
            @test r.done == length(ks) - nbig
            big = [e for e in _pl_events(outdir) if e.kind == "key_too_big"]
            @test length(big) == nbig
            @test all(e -> e.cores == 64 && e.node_cores == 4, big)
        end
    end
end

@testset "a key whose worker died is retried with more memory" begin
    _pl_pool(; key_req=k -> KeyReq(1, 1.0), mem_growth=2.0) do pool
        _pl_vault() do v, outdir
            k = DataVault.keys(v)[1]
            died = joinpath(outdir, "died")
            work = key -> begin
                if !isfile(died)
                    touch(died)
                    ccall(:_exit, Cvoid, (Cint,), 1)
                end
                return Dict{String,Any}("x" => 1)
            end
            r = run!(work, v, [k]; pool=pool)
            @test r.done == 1
            ev = only([e for e in _pl_events(outdir) if e.kind == "pool_retry_mem"])
            @test (ev.had_gb, ev.next_gb) == (1.0, 2.0)
            # The second worker was started with the larger request.
            @test any(e -> e.kind == "pool_spawn" && e.mem_gb == 2.0, _pl_events(outdir))
        end
    end
end

# ── the paths a reviewer found untested (#99, #101, #102, #103, #115) ────────────────────────────

# A spawner that offers one node and starts workers the way `starter` says.
struct _PlSpawner <: SweepRunner.Spawner
    node::PoolNode
    starter::Any
end
SweepRunner.pool_nodes(s::_PlSpawner) = [s.node]
function SweepRunner.start_workers(s::_PlSpawner, node, size, n; exeflags)
    return s.starter(node, size, n, exeflags)
end

function _pl_local(node, size, n, exeflags)
    return SweepRunner.start_workers(
        LocalSpawner(; cores=4, mem_gb=8.0), node, size, n; exeflags=exeflags
    )
end

function _pl_custom(f, starter; cores=4, mem_gb=8.0, kw...)
    nprocs() > 1 && rmprocs(workers())
    pool = SizedPool(
        _PlSpawner(PoolNode(gethostname(), cores, mem_gb), starter);
        key_req=k -> KeyReq(1, 1.0),
        poll=0.05,
        keep=true,
        kw...,
    )
    try
        f(pool)
    finally
        shutdown!(pool)
        nprocs() > 1 && rmprocs(workers())
        note_workers!(; planned=0, launched=0)
    end
end

@testset "KeyReq, PoolNode and SizedPool refuse values that would break the planner" begin
    @test_throws ArgumentError KeyReq(0, 1.0)
    @test_throws ArgumentError KeyReq(-1, 1.0)
    @test_throws ArgumentError KeyReq(1, 0.0)
    @test_throws ArgumentError KeyReq(1, NaN)
    @test_throws ArgumentError KeyReq(1, -2.0)
    @test_throws ArgumentError PoolNode("n", -1, 1.0)
    @test_throws ArgumentError PoolNode("n", 1, NaN)
    @test PoolNode("n", 0, 0.0).cores == 0                    # a node with nothing left is fine
    mk(; kw...) = SizedPool(LocalSpawner(); key_req=k -> KeyReq(1, 1.0), kw...)
    @test_throws ArgumentError mk(; max_workers=0)
    @test_throws ArgumentError mk(; max_threads=0)
    @test_throws ArgumentError mk(; mem_growth=1.0)
    @test_throws ArgumentError mk(; poll=0)
    @test_throws ArgumentError mk(; retire_after=-1)
end

@testset "the per-master limit applies to the pool, and an unreadable one is a default (#99)" begin
    lim = SweepRunner._pool_limit
    slurm = SlurmStepSpawner([PoolNode("c01", 8, 16.0)])
    withenv("SWEEPRUNNER_MAX_WORKERS" => nothing) do
        @test lim(LocalSpawner(), nothing) == (typemax(Int), "none")
        @test lim(LocalSpawner(), 7) == (7, "max_workers")
        @test lim(slurm, nothing; limit=() -> 1607) == (1607, "SrunPortRange")
        # Under Slurm, "could not read the limit" is a cap, not no cap.
        n, why = lim(slurm, nothing; limit=() -> nothing)
        @test n == 1500 && occursin("could not be read", why)
        @test lim(slurm, 40; limit=() -> 1607) == (40, "max_workers")
    end
    withenv("SWEEPRUNNER_MAX_WORKERS" => "250") do
        @test lim(slurm, nothing; limit=() -> 1607) == (250, "SWEEPRUNNER_MAX_WORKERS")
    end
end

@testset "SlurmStepSpawner: a master's slice, the allocation's memory, the srun reservation (#115)" begin
    env = Dict(
        "SLURM_JOB_NODELIST" => "c[01-04]",
        "SLURM_JOB_CPUS_PER_NODE" => "128(x4)",
        "SLURM_MEM_PER_NODE" => "8000",                       # the STEP's --mem, not the node's
    )
    ns = SweepRunner._slurm_pool_nodes(
        env,
        "c03";
        only=Set(["c03", "c04"]),
        master_gb=3.0,
        headroom_gb=4.0,
        srun_gb=0.008,
        mem_per_node_mb=230000,
    )
    @test [n.name for n in ns] == ["c03", "c04"]
    @test ns[2].mem_gb ≈ 230000 / 1024 - 4.0                   # the allocation's figure
    # The master's node also keeps room for the srun clients of its slice's workers.
    @test ns[1].mem_gb ≈ 230000 / 1024 - 4.0 - (3.0 + 0.008 * 256)
    @test ns[1].cores == 127
    @test_throws ErrorException SweepRunner._slurm_pool_nodes(
        env, "c01"; only=Set(["zz"]), master_gb=3.0, headroom_gb=4.0
    )
end

@testset "a worker is not handed work before the pool knows its size (#101)" begin
    pool = SizedPool(LocalSpawner(; cores=4, mem_gb=8.0); key_req=k -> KeyReq(4, 6.0))
    row = TaskTable([ParamIO.DataKey(Dict{String,Any}("N" => 1), 1)]).rows[1]
    # Visible in workers() but not registered: neither adopted nor accepted.
    @test !SweepRunner._pool_adoptable(pool, 99)
    @test !SweepRunner._pool_accepts(pool, 99, row, nothing, nothing)
    # A worker that was there before the pool is not the pool's, and takes anything.
    push!(pool.foreign, 99)
    @test SweepRunner._pool_adoptable(pool, 99)
    @test SweepRunner._pool_accepts(pool, 99, row, nothing, nothing)
    # One the pool registered takes what its size holds, and nothing once it is retiring.
    pool.workers[7] = SweepRunner.PoolWorker("n", KeyReq(1, 1.0), time(), false)
    @test !SweepRunner._pool_accepts(pool, 7, row, nothing, nothing)
    pool.workers[8] = SweepRunner.PoolWorker("n", KeyReq(4, 6.0), time(), false)
    @test SweepRunner._pool_accepts(pool, 8, row, nothing, nothing)
    pool.workers[8].retiring = true
    @test !SweepRunner._pool_accepts(pool, 8, row, nothing, nothing)
    @test SweepRunner._pool_retiring(pool, 8)
end

@testset "memory after a second death grows from what the key last had (#101)" begin
    pool = SizedPool(LocalSpawner(; cores=4, mem_gb=64.0); key_req=k -> KeyReq(1, 2.0))
    row = TaskTable([ParamIO.DataKey(Dict{String,Any}("N" => 1), 1)]).rows[1]
    log = SweepRunner.EventLog(joinpath(mktempdir(), "e.jsonl"))
    # The worker is already forgotten by the pool both times (the tick freed it first).
    SweepRunner._pool_death!(pool, row, 5, log, :s)
    @test pool.memreq[row.kstr] == 3.0
    SweepRunner._pool_death!(pool, row, 6, log, :s)
    @test pool.memreq[row.kstr] == 4.5                         # from 3.0, not from 2.0 again
    for _ in 1:20
        SweepRunner._pool_death!(pool, row, 6, log, :s)
    end
    @test pool.memreq[row.kstr] == 64.0                        # never past what a node has
end

@testset "a worker that cannot be readied is removed, and fails alone (#101, #115)" begin
    nprocs() > 1 && rmprocs(workers())
    ids = addprocs(2; exeflags="--project=$(dirname(Base.active_project()))")
    try
        bad = ids[1]
        ready = (w, size) -> w == bad ? error("cannot load") : nothing
        good = SweepRunner._ready_workers!(ids, KeyReq(1, 1.0); ready=ready)
        @test good == [ids[2]]
        @test !(bad in procs())                                # not left running
        @test ids[2] in procs()
    finally
        nprocs() > 1 && rmprocs(workers())
    end
end

@testset "starts that fail are logged, and ten in a row is an error, not a quiet end (#101)" begin
    _pl_custom((node, size, n, flags) -> error("no such partition")) do pool
        _pl_vault() do v, outdir
            ks = DataVault.keys(v)
            err = try
                run!(k -> Dict{String,Any}("x" => 1), v, ks; pool=pool)
                nothing
            catch e
                e
            end
            @test err isa ErrorException
            @test occursin("worker starts failed in a row", err.msg)
            ev = _pl_events(outdir)
            failed = [e for e in ev if e.kind == "pool_spawn_failed"]
            @test length(failed) >= 10
            @test occursin("no such partition", failed[1].err)
            @test count(e -> e.kind == "pool_gave_up", ev) == 1
            # Nothing is left starting, and the room is all back.
            @test isempty(pool.starting)
            @test pool.free_c[gethostname()] == 4
            @test all(k -> !DataVault.is_done(v, k), ks)
        end
    end
end

@testset "a start that brings fewer workers than asked is said, and the round goes on (#101)" begin
    short = (node, size, n, flags) -> _pl_local(node, size, 1, flags)    # one, whatever was asked
    _pl_custom(short) do pool
        _pl_vault() do v, outdir
            ks = DataVault.keys(v)
            r = run!(k -> (sleep(0.2); Dict{String,Any}("x" => 1)), v, ks; pool=pool)
            @test r.done == length(ks)
            ev = [e for e in _pl_events(outdir) if e.kind == "pool_spawn_short"]
            @test !isempty(ev)
            @test all(e -> e.started == 1 && e.asked > 1, ev)
            # The room of the workers that did not come is back: used == what the workers hold.
            used = sum(w.size.cores for w in values(pool.workers); init=0)
            @test pool.free_c[gethostname()] == 4 - used
        end
    end
end

@testset "max_workers is never exceeded, and reaching it is said once (#99)" begin
    _pl_custom(_pl_local; max_workers=2) do pool
        _pl_vault() do v, outdir
            ks = DataVault.keys(v)
            most = Ref(0)
            watching = Ref(true)
            watcher = @async while watching[]
                held = length(pool.workers) + sum(x -> x[3], values(pool.starting); init=0)
                most[] = max(most[], held)
                sleep(0.02)
            end
            r = run!(k -> (sleep(0.3); Dict{String,Any}("x" => 1)), v, ks; pool=pool)
            watching[] = false
            wait(watcher)
            @test r.done == length(ks)
            @test most[] == 2
            ev = _pl_events(outdir)
            @test count(e -> e.kind == "pool_at_limit", ev) == 1
            lim = only([e for e in ev if e.kind == "pool_limit"])
            @test lim.max_workers == 2 && lim.source == "max_workers"
        end
    end
end

@testset "the pool follows the master: paused, stopping, a drained node, a resize target (#102)" begin
    never = (node, size, n, flags) -> Int[]
    function tick(setup; fits=Returns(true), deadline=nothing)
        started = Ref(-1)
        _pl_custom(never) do pool
            _pl_vault() do v, outdir
                table = TaskTable(DataVault.keys(v))
                m = SweepRunner.Master()
                m.multi = true
                setup(m)
                log = SweepRunner.EventLog(joinpath(outdir, "e.jsonl"))
                opts = RunOpts(; deadline=deadline, stop_flag=nothing)
                SweepRunner._pool_tick!(pool, table, m, log, :s, opts, nothing; fits=fits)
                # Counted before the start tasks run: this is what the tick decided.
                started[] = sum(x -> x[3], values(pool.starting); init=0)
            end
        end
        return started[]
    end
    @test tick(m -> nothing) == 4                              # four keys, four cores: four starts
    @test tick(m -> (m.ctl.paused = true)) == 0
    @test tick(m -> (m.ctl.stop_all = true)) == 0
    @test tick(m -> push!(m.ctl.drained, gethostname())) == 0
    @test tick(m -> (m.ctl.target = 1)) == 1
    @test tick(m -> (m.ctl.target = 0)) == 0
    # Keys the deadline will hold back are not started for.
    @test tick(m -> nothing; fits=k -> k.params["N"] == 4) == 2
    @test tick(m -> nothing; fits=Returns(false)) == 0
end

@testset "a key :finish_by gave more cores to is run, not held back for its one-core time (#103)" begin
    nprocs() > 1 && rmprocs(workers())
    pool = SizedPool(
        LocalSpawner(; cores=4, mem_gb=8.0);
        key_req=k -> KeyReq(1, 1.0),
        threads=:finish_by,
        speedup=(k, c) -> Float64(c),
        max_threads=4,
        poll=0.2,
    )
    try
        _pl_vault() do v, _
            k = DataVault.keys(v)[1]
            # 150 s at one core, 120 s left: it only fits at two cores or more.
            need = key -> 150.0
            work =
                key -> Dict{String,Any}("threads" => LinearAlgebra.BLAS.get_num_threads())
            r = run!(
                work,
                v,
                [k];
                pool=pool,
                load=:LinearAlgebra,
                min_time=need,
                opts=RunOpts(; deadline=time() + 120),
            )
            @test (r.done, r.held_back) == (1, 0)
            @test DataVault.load(v, k)["threads"] >= 2
            @test SweepRunner._pool_min_time(pool, k, time() + 120, need) <= 75.0
        end
    finally
        shutdown!(pool)
        nprocs() > 1 && rmprocs(workers())
        note_workers!(; planned=0, launched=0)
    end
end

@testset "a pool's workers go when the call that was given the pool returns (#101)" begin
    nprocs() > 1 && rmprocs(workers())
    pool = SizedPool(
        LocalSpawner(; cores=2, mem_gb=4.0); key_req=k -> KeyReq(1, 1.0), poll=0.2
    )
    try
        _pl_vault() do v, _
            ks = DataVault.keys(v)
            r = run_loop!(k -> Dict{String,Any}("x" => 1), v, ks; pool=pool)
            @test r.done == length(ks)
            @test isempty(pool.workers)
            @test nprocs() == 1
            @test pool.free_c[gethostname()] == 2
        end
    finally
        shutdown!(pool)
        nprocs() > 1 && rmprocs(workers())
    end
end

@testset "an idle worker is retired for a key that needs its room, and a busy one is kept (#111)" begin
    # One node of 4 cores: a small worker holds a core the large key needs all of.
    _pl_pool(;
        key_req=k -> k.params["N"] == 8 ? KeyReq(4, 6.0) : KeyReq(1, 1.0), retire_after=0.3
    ) do pool
        _pl_vault() do v, outdir
            ks = DataVault.keys(v)
            work = k -> (sleep(0.3); Dict{String,Any}("pid" => Distributed.myid()))
            r = run!(work, v, ks; pool=pool, load=[:Distributed])
            @test r.done == length(ks)
            @test (r.err, r.busy, r.gave_up) == (0, 0, 0)     # no key lost to a retired worker
            ev = _pl_events(outdir)
            retired = [e for e in ev if e.kind == "pool_retire"]
            @test !isempty(retired)
            # Both sizes ran, which the node cannot hold at once.
            spawned = [e for e in ev if e.kind == "pool_spawn"]
            @test Set(e.cores for e in spawned) == Set([1, 4])
            # No key was running on a worker when it was retired.
            lost = [e for e in ev if e.kind in ("worker_lost", "key_requeued")]
            @test isempty(lost)
            # The room is what the remaining workers hold: retiring gave it back.
            used = sum((w.size.cores for w in values(pool.workers)); init=0)
            @test pool.free_c[gethostname()] == 4 - used
        end
    end
end

@testset "with a pool, a key that cannot fit before the deadline gets no worker (#111)" begin
    _pl_pool(; key_req=k -> k.params["N"] == 8 ? KeyReq(2, 3.0) : KeyReq(1, 1.0)) do pool
        _pl_vault() do v, outdir
            ks = DataVault.keys(v)
            nbig = count(k -> k.params["N"] == 8, ks)
            need = k -> k.params["N"] == 8 ? 1000.0 : 0.01
            r = run!(
                k -> Dict{String,Any}("x" => 1),
                v,
                ks;
                pool=pool,
                opts=RunOpts(; deadline_in=60),
                min_time=need,
            )
            @test r.held_back == nbig
            @test r.done == length(ks) - nbig
            @test (r.err, r.busy) == (0, 0)
            spawned = [e for e in _pl_events(outdir) if e.kind == "pool_spawn"]
            @test !isempty(spawned)
            @test all(e -> e.cores == 1, spawned)             # none of the large size
        end
    end
end

@testset "a key that always kills its worker is given up on, in bounded starts and memory (#111)" begin
    # One node of 4 cores and 8 GB; the key asks for 1 GB and doubles after each death.
    _pl_pool(; key_req=k -> KeyReq(1, 1.0), mem_growth=2.0) do pool
        _pl_vault() do v, outdir
            ks = DataVault.keys(v)[1:2]
            bad = ParamIO.canonical(ks[1])
            work =
                key -> begin
                    ParamIO.canonical(key) == bad && ccall(:_exit, Cvoid, (Cint,), 1)
                    return Dict{String,Any}("x" => 1)
                end
            r = run!(work, v, ks; pool=pool)
            # The key is reported, the other one is not taken down with it, and the run ends.
            @test r.done == 1
            @test (r.err, r.gave_up) == (1, 1)
            @test DataVault.is_done(v, ks[2]) && !DataVault.is_done(v, ks[1])
            @test !DataVault.is_running(v, ks[1])               # its lock did not stay behind
            ev = _pl_events(outdir)
            # It took down a bounded number of workers...
            deaths = SweepRunner._WORKER_DEATH_REDISPATCHES + 1
            started = sum(e.n for e in ev if e.kind == "pool_spawn")
            @test started <= deaths + 2                         # and one for the good key
            # ...and what it was given never passed what the node has.
            retries = [e for e in ev if e.kind == "pool_retry_mem"]
            @test length(retries) <= deaths
            @test all(e -> e.next_gb <= 8.0, retries)
            @test all(e -> e.mem_gb <= 8.0, [e for e in ev if e.kind == "pool_spawn"])
            # The room of every worker that died came back.
            used = sum((w.size.cores for w in values(pool.workers)); init=0)
            @test pool.free_c[gethostname()] == 4 - used
            @test pool.free_m[gethostname()] ≈
                8.0 - sum((w.size.mem_gb for w in values(pool.workers)); init=0.0)
        end
    end
end
