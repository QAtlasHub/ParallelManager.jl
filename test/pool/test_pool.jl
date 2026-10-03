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
    pool = SizedPool(LocalSpawner(; cores, mem_gb); poll=0.2, kw...)
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
