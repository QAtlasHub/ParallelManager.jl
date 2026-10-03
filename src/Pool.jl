# Pool — workers sized to the keys they run.
#
# `init_workers!` starts `n` identical workers and `run!` hands any key to any of them, so a sweep
# whose keys differ in size has to size every worker for its largest key. Measured downstream
# (128 cores, 226 GB per node; keys from 1 core / 2 GB to 7 cores / 12 GB): sized per key a node
# held ~55 small workers, sized for the largest it held 18.
#
# Here a key says what it needs (`key_req(key) -> KeyReq(cores, mem_gb)`), and the pool starts a
# worker of that size where a node has the room, reuses it for the next key it fits, and retires
# it when its size has nothing left to do and another size is waiting. A worker that dies under a
# key (out of memory) has the key retried with more memory.
#
# Correctness does not rest on the pool. A key still goes through the per-key pipeline (the lock,
# the heartbeat, the commit), so a pool's wrong view of the room costs a wasted start and nothing
# else.
#
# The pool, its planner (`plan_spawns`, a pure function) and the local spawner are exercised by
# the tests. `SlurmStepSpawner` and `StepManager` are the downstream implementation moved here;
# they need an allocation to run and have not been run from this package.

using Distributed
using LinearAlgebra: BLAS

"""
    KeyReq(cores, mem_gb)

What one key needs from the worker that runs it, and the size a worker was started with.
"""
struct KeyReq
    cores::Int
    mem_gb::Float64
end

KeyReq(cores::Integer, mem_gb::Real) = KeyReq(Int(cores), Float64(mem_gb))

# Can a worker of size `have` run a key that needs `need`?
_fits(have::KeyReq, need::KeyReq) = need.cores <= have.cores && need.mem_gb <= have.mem_gb

"""
    PoolNode(name, cores, mem_gb)

A node workers can be started on, and what it offers.
"""
struct PoolNode
    name::String
    cores::Int
    mem_gb::Float64
end

"""
    Spawner

Where a [`SizedPool`](@ref)'s workers come from. A backend implements `pool_nodes(s)` (the
[`PoolNode`](@ref)s it offers) and `start_workers(s, node, size, n; exeflags)` (start `n` workers
of [`KeyReq`](@ref) `size` on `node`, return their Distributed ids). [`LocalSpawner`](@ref) and
[`SlurmStepSpawner`](@ref) are provided.
"""
abstract type Spawner end

"""
    LocalSpawner(; cores=Sys.CPU_THREADS - 1, mem_gb=0.8 * total memory)

Workers are local `julia --worker` processes on this machine, treated as one node. For tests and
a workstation; nothing enforces a worker's memory limit.
"""
struct LocalSpawner <: Spawner
    node::PoolNode
end

function LocalSpawner(;
    cores::Integer=max(Sys.CPU_THREADS - 1, 1), mem_gb::Real=0.8 * Sys.total_memory() / 2^30
)
    return LocalSpawner(PoolNode(gethostname(), Int(cores), Float64(mem_gb)))
end

"""
    SlurmStepSpawner(; nodes=<the allocation>, master_gb=3.0, headroom_gb=1.0)

Workers are job steps of the current allocation: `srun --exact -N1 -n1 --nodelist=<node>
--cpus-per-task=<cores> --mem=<mem>`, so each worker is its own step with its own memory cgroup.
Every node offers its CPUs and memory, less `headroom_gb`; the master's node also keeps one core
and `master_gb` for the master.

`nodes` limits it to part of the allocation (a master of a node group, see
[`split_nodes`](@ref)).
"""
struct SlurmStepSpawner <: Spawner
    nodes::Vector{PoolNode}
end

# `128(x2),64` -> [128, 128, 64]
function _expand_slurm_counts(s::AbstractString, n::Int)
    out = Int[]
    for part in split(s, ','; keepempty=false)
        m = match(r"^(\d+)(?:\(x(\d+)\))?$", strip(part))
        m === nothing && error("SLURM_JOB_CPUS_PER_NODE: cannot read $(repr(s))")
        append!(out, fill(parse(Int, m[1]), m[2] === nothing ? 1 : parse(Int, m[2])))
    end
    length(out) == n || error("SLURM_JOB_CPUS_PER_NODE $(repr(s)) does not cover $n nodes")
    return out
end

# The allocation's nodes with what each offers, from the environment a Slurm job has.
function _slurm_pool_nodes(
    env::AbstractDict, me::AbstractString; only=nothing, master_gb::Real, headroom_gb::Real
)
    names = expand_nodelist(env["SLURM_JOB_NODELIST"])
    isempty(names) &&
        error("SLURM_JOB_NODELIST $(repr(env["SLURM_JOB_NODELIST"])) not read")
    cpus = _expand_slurm_counts(get(env, "SLURM_JOB_CPUS_PER_NODE", ""), length(names))
    mem_mb = if haskey(env, "SLURM_MEM_PER_NODE")
        fill(parse(Float64, env["SLURM_MEM_PER_NODE"]), length(names))
    elseif haskey(env, "SLURM_MEM_PER_CPU")
        parse(Float64, env["SLURM_MEM_PER_CPU"]) .* cpus
    else
        error("neither SLURM_MEM_PER_NODE nor SLURM_MEM_PER_CPU is set: node memory unknown")
    end
    nodes = PoolNode[]
    for (n, c, m) in zip(names, cpus, mem_mb)
        (only === nothing || n in only) || continue
        master = n == me || startswith(me, n * ".")
        push!(
            nodes,
            PoolNode(
                n,
                c - (master ? 1 : 0),
                max(0.0, m / 1024 - headroom_gb - (master ? master_gb : 0.0)),
            ),
        )
    end
    isempty(nodes) && error("none of the nodes asked for is in the allocation")
    return nodes
end

function SlurmStepSpawner(; nodes=nothing, master_gb::Real=3.0, headroom_gb::Real=1.0)
    only = nodes === nothing ? nothing : Set(String.(nodes))
    return SlurmStepSpawner(
        _slurm_pool_nodes(ENV, gethostname(); only, master_gb, headroom_gb)
    )
end

pool_nodes(s::LocalSpawner) = [s.node]
pool_nodes(s::SlurmStepSpawner) = s.nodes

"""
    StepManager(node, cores, mem_gb, srun, n)

A `ClusterManager` that starts `n` workers of one size on `node`, each inside its own `srun`
step when `srun` is true, as plain local processes otherwise.

`n > 1` matters: `addprocs` holds Distributed's worker lock for the whole call, so one call per
worker starts them strictly one after another; within ONE call the launched workers are connected
and set up concurrently.
"""
struct StepManager <: ClusterManager
    node::String
    cores::Int
    mem_gb::Float64
    srun::Bool
    n::Int
end

# On an allocation that spans racks a node's default address can be one the master cannot reach;
# its host name resolves to the one every node can. Unchanged where the name does not resolve.
function _bind_flag(node::AbstractString)
    ip = try
        Distributed.Sockets.getaddrinfo(node, Distributed.Sockets.IPv4)
    catch
        return ``
    end
    return `--bind-to $ip`
end

# The command that starts one worker of `m`'s size.
function _step_command(m::StepManager, worker::Cmd)
    m.srun || return worker
    mb = ceil(Int, m.mem_gb * 1024)
    return `srun --exact --nodes=1 --ntasks=1 --nodelist=$(m.node) --cpus-per-task=$(m.cores) --mem=$(mb)M --cpu-bind=cores --kill-on-bad-exit=1 $worker`
end

function Distributed.launch(m::StepManager, params::Dict, launched::Array, c::Condition)
    exename = params[:exename]
    exeflags = params[:exeflags]
    bind = m.srun ? _bind_flag(m.node) : ``
    cmd = _step_command(m, `$(Base.julia_cmd(exename)) $exeflags $bind --worker`)
    env = Dict{String,String}(ENV)
    # The allocation's own per-cpu memory would contradict --mem on the step.
    for v in ("SLURM_MEM_PER_CPU", "SLURM_MEM_PER_NODE", "SLURM_MEM_PER_GPU")
        delete!(env, v)
    end
    env["OPENBLAS_NUM_THREADS"] = string(m.cores)
    env["MKL_NUM_THREADS"] = string(m.cores)
    env["JULIA_NUM_THREADS"] = "1"
    # What the worker reports as its cores (status, account, cost records).
    env["SLURM_CPUS_PER_TASK"] = string(m.cores)
    project = Base.ACTIVE_PROJECT[]
    project === nothing || (env["JULIA_PROJECT"] = project)
    env["JULIA_LOAD_PATH"] = join(LOAD_PATH, ":")
    env["JULIA_DEPOT_PATH"] = join(DEPOT_PATH, ":")
    for _ in 1:(m.n)
        io = open(detach(setenv(cmd, env; dir=params[:dir])), "r+")
        Distributed.write_cookie(io)
        wc = WorkerConfig()
        wc.process = io
        wc.io = io.out
        wc.enable_threaded_blas = true
        push!(launched, wc)
    end
    return notify(c)
end

function Distributed.manage(::StepManager, ::Integer, config::WorkerConfig, op::Symbol)
    op === :interrupt && config.process !== nothing && kill(something(config.process), 2)
    return nothing
end

"""
    start_workers(spawner, node, size, n; exeflags) -> Vector{Int}

Start `n` workers of `size` on `node` and return their Distributed ids, each with its BLAS
threads set to `size.cores`. Fewer ids than `n` means the rest did not start.
"""
function start_workers(s::Spawner, node::AbstractString, size::KeyReq, n::Integer; exeflags)
    ids = addprocs(
        StepManager(String(node), size.cores, size.mem_gb, s isa SlurmStepSpawner, Int(n));
        exeflags=exeflags,
    )
    isempty(ids) && return ids
    # The package first: a fresh worker has loaded nothing, and cannot even be told what to run.
    Distributed.remotecall_eval(Main, ids, :(using SweepRunner))
    for w in ids
        remotecall_fetch(_set_blas_threads, w, size.cores)
    end
    return ids
end

_set_blas_threads(n::Integer) = (BLAS.set_num_threads(n); nothing)

# ── the planner ─────────────────────────────────────────────────────────────────────────────────

"""
    worker_size(need, node, free_cores, free_mem; threads=:throughput, max_threads=8) -> KeyReq

The size a worker is started with for a key that needs `need`, on a node with that much room.

- `:throughput` — the cores its memory stands for on this node
  (`need.mem_gb × free cores / free memory`), at least what the key declares and at most
  `max_threads`. Where memory binds before cores, a key sized by its cores alone strands the
  rest of the node; this hands those cores to the keys whose memory holds them.
- `:fastest` — up to `max_threads` cores, and the memory that comes with them on this node. For
  allocations where cores are not the scarce thing, or a key that has to finish.
"""
function worker_size(
    need::KeyReq,
    node::PoolNode,
    free_c::Integer,
    free_m::Real;
    threads::Symbol=:throughput,
    max_threads::Integer=8,
)
    if threads === :fastest
        c = max(need.cores, min(max_threads, free_c))
        return KeyReq(c, max(need.mem_gb, c * node.mem_gb / max(node.cores, 1)))
    end
    free_m > 0 || return need
    c = round(Int, need.mem_gb * free_c / free_m)
    return KeyReq(
        clamp(c, need.cores, max(need.cores, min(free_c, max_threads))), need.mem_gb
    )
end

"""
    plan_spawns(needs, waited, nodes, free_cores, free_mem, covering; threads, max_threads,
                starve_after, room) -> (; starts, blocked)

Which workers to start. `needs` are the queued keys' requirements in queue order and `waited`
how long each has found no room; `covering` are the sizes of the workers that are idle or already
starting, each of which will take one key it fits.

In order: a need that a covering worker fits takes it; otherwise the worker is started on the
node that keeps the most memory free after it; a need that fits nowhere is `blocked`. Smaller
needs behind a blocked one still start (backfill) until it has waited `starve_after` seconds:
from then on nothing is started ahead of it, so the room it needs is freed by keys finishing.
At most `room` workers are planned.

Pure: `free_cores` / `free_mem` are not modified. `starts` is a vector of `(node, size)`,
`blocked` the indices into `needs`.
"""
function plan_spawns(
    needs::AbstractVector{KeyReq},
    waited::AbstractVector{<:Real},
    nodes::AbstractVector{PoolNode},
    free_c::AbstractDict,
    free_m::AbstractDict,
    covering::AbstractVector{KeyReq};
    threads::Symbol=:throughput,
    max_threads::Integer=8,
    starve_after::Real=600.0,
    room::Integer=typemax(Int),
)
    fc, fm = copy(free_c), copy(free_m)
    cover = collect(covering)
    starts = Tuple{String,KeyReq}[]
    blocked = Int[]
    for (i, need) in enumerate(needs)
        j = findfirst(c -> _fits(c, need), cover)
        if j !== nothing
            deleteat!(cover, j)
            continue
        end
        best, bestroom, size = nothing, -Inf, need
        for n in nodes
            s = worker_size(need, n, fc[n.name], fm[n.name]; threads, max_threads)
            (fc[n.name] >= s.cores && fm[n.name] >= s.mem_gb) || continue
            left = fm[n.name] - s.mem_gb
            left > bestroom && ((best, bestroom, size) = (n.name, left, s))
        end
        if best === nothing
            push!(blocked, i)
            waited[i] >= starve_after && break
            continue
        end
        length(starts) >= room && break
        push!(starts, (best, size))
        fc[best] -= size.cores
        fm[best] -= size.mem_gb
    end
    return (; starts, blocked)
end

# ── the pool ────────────────────────────────────────────────────────────────────────────────────

mutable struct PoolWorker
    const node::String
    const size::KeyReq
    last_busy::Float64
    retiring::Bool
end

"""
    SizedPool(spawner=default_spawner(); key_req, threads=:throughput, max_threads=8,
              speedup=(key, cores) -> 1.0, retire_after=120.0, starve_after=600.0,
              mem_growth=1.5, max_workers=typemax(Int), poll=1.0, exeflags=<project, -t1>)

A pool of workers sized to the keys they run. Give it to [`run!`](@ref) / [`run_loop!`](@ref) as
`pool=`; it starts workers as the queue needs them (no `init_workers!`), and they stay for the
next round. [`shutdown!`](@ref) removes them.

- `key_req` — `key -> KeyReq(cores, mem_gb)`: what a key needs.
- `threads` — how many cores a worker is given beyond what its key declares
  ([`worker_size`](@ref)): `:throughput` (the cores its memory stands for; most work per
  node-hour), `:fastest` (up to `max_threads`; time-to-solution), or `:finish_by` (as
  `:throughput`, but a key that would not reach its next checkpoint before the job's `deadline`
  is given the cores that get it there, by `speedup`).
- `speedup` — `(key, cores) -> factor` relative to one core, for `:finish_by`
  ([`measured_speedup`](@ref) builds one from the cost records).
- `retire_after` — an idle worker whose size no queued key fits is retired after this long, when
  another size is waiting for room.
- `starve_after` — how long a key that fits nowhere lets smaller keys start ahead of it.
- `mem_growth` — a key whose worker died is retried with this much more memory (up to what a
  node has), at most `RunOpts`' death bound times.
- `max_workers` — the most workers this pool holds at once (under Slurm, the per-master limit:
  [`srun_worker_limit`](@ref)).
"""
mutable struct SizedPool
    const spawner::Spawner
    const key_req::Any
    const threads::Symbol
    const max_threads::Int
    const speedup::Any
    const retire_after::Float64
    const starve_after::Float64
    const mem_growth::Float64
    const max_workers::Int
    const poll::Float64
    const exeflags::Cmd
    const nodes::Vector{PoolNode}
    const free_c::Dict{String,Int}
    const free_m::Dict{String,Float64}
    const workers::Dict{Int,PoolWorker}
    const starting::Dict{Int,Tuple{String,KeyReq,Int}}     # token => (node, size, how many)
    const memreq::Dict{String,Float64}                     # raised after a worker died on it
    const waiting::Dict{String,Float64}                    # since when a key has found no room
    const too_big::Set{String}
    seq::Int
    fails::Int
    stuck::Bool
end

"""
    default_spawner() -> Spawner

[`SlurmStepSpawner`](@ref) inside a Slurm job, [`LocalSpawner`](@ref) otherwise.
"""
default_spawner() = haskey(ENV, "SLURM_JOB_ID") ? SlurmStepSpawner() : LocalSpawner()

function _default_exeflags()
    project = dirname(something(Base.active_project(), "."))
    img = _worker_exeflags(get(ENV, "SWEEPRUNNER_SYSIMAGE", nothing))
    return `--project=$project -t1 $img`
end

function SizedPool(
    spawner::Spawner=default_spawner();
    key_req,
    threads::Symbol=:throughput,
    max_threads::Integer=8,
    speedup=(key, cores) -> 1.0,
    retire_after::Real=120.0,
    starve_after::Real=600.0,
    mem_growth::Real=1.5,
    max_workers::Integer=typemax(Int),
    poll::Real=1.0,
    exeflags::Cmd=_default_exeflags(),
)
    threads in (:throughput, :fastest, :finish_by) || throw(
        ArgumentError(
            "SizedPool: threads must be :throughput, :fastest or :finish_by, got " *
            repr(threads),
        ),
    )
    nodes = pool_nodes(spawner)
    isempty(nodes) && throw(ArgumentError("SizedPool: the spawner offers no node"))
    return SizedPool(
        spawner,
        key_req,
        threads,
        Int(max_threads),
        speedup,
        Float64(retire_after),
        Float64(starve_after),
        Float64(mem_growth),
        Int(max_workers),
        Float64(poll),
        exeflags,
        nodes,
        Dict(n.name => n.cores for n in nodes),
        Dict(n.name => n.mem_gb for n in nodes),
        Dict{Int,PoolWorker}(),
        Dict{Int,Tuple{String,KeyReq,Int}}(),
        Dict{String,Float64}(),
        Dict{String,Float64}(),
        Set{String}(),
        0,
        0,
        false,
    )
end

# What `row`'s key needs now: its declared size, the memory raised after a death, and under
# `:finish_by` the cores that get it to its next checkpoint before the deadline.
function _pool_need(pool::SizedPool, row::TaskRow, deadline, min_time)::KeyReq
    base = pool.key_req(row.key)
    base = KeyReq(base.cores, max(base.mem_gb, get(pool.memreq, row.kstr, 0.0)))
    (pool.threads === :finish_by && deadline !== nothing && min_time !== nothing) ||
        return base
    left = deadline - time()
    need = _seconds_or(min_time, row.key, 0.0)
    s0 = max(Float64(pool.speedup(row.key, base.cores)), 1e-9)
    for c in base.cores:max(pool.max_threads, base.cores)
        need * s0 / max(Float64(pool.speedup(row.key, c)), 1e-9) <= left &&
            return KeyReq(c, base.mem_gb)
    end
    return KeyReq(max(pool.max_threads, base.cores), base.mem_gb)
end

# May worker `pid` take `row`? A worker the pool did not start takes anything, as before.
function _pool_accepts(pool::SizedPool, pid::Int, row::TaskRow, deadline, min_time)::Bool
    w = get(pool.workers, pid, nothing)
    w === nothing && return true
    w.retiring && return false
    return _fits(w.size, _pool_need(pool, row, deadline, min_time))
end

_pool_threads(pool::SizedPool) = pool.threads === :fastest ? :fastest : :throughput

# A worker died under `row`: most often its memory. The key comes back asking for more.
function _pool_death!(pool::SizedPool, row::TaskRow, pid::Int, log::EventLog, stage::Symbol)
    w = get(pool.workers, pid, nothing)
    had = w === nothing ? pool.key_req(row.key).mem_gb : w.size.mem_gb
    cap = maximum(n.mem_gb for n in pool.nodes)
    pool.memreq[row.kstr] = min(pool.mem_growth * had, cap)
    log_event(
        log,
        :pool_retry_mem;
        level=:warn,
        stage=stage,
        key=row.kstr,
        had_gb=round(had; digits=2),
        next_gb=round(pool.memreq[row.kstr]; digits=2),
    )
    return nothing
end

# Give a worker's room back and forget it.
function _pool_free!(pool::SizedPool, pid::Int)
    w = pop!(pool.workers, pid, nothing)
    w === nothing && return nothing
    pool.free_c[w.node] += w.size.cores
    pool.free_m[w.node] += w.size.mem_gb
    return nothing
end

"""
    _pool_tick!(pool, table, master, log, stage, opts, min_time)

One pass of the pool: forget workers that are gone, report keys no node can hold, start the
workers the queue needs ([`plan_spawns`](@ref)), and retire idle workers whose size is no longer
wanted while another size waits for room.
"""
function _pool_tick!(
    pool::SizedPool,
    table::TaskTable,
    master::Master,
    log::EventLog,
    stage::Symbol,
    opts::RunOpts,
    min_time,
)
    now = time()
    live = Set(procs())
    for pid in collect(keys(pool.workers))
        pid in live || _pool_free!(pool, pid)
    end

    # What is on each worker, and the queued rows in queue order (a bounded look ahead: more
    # than the nodes could ever hold at once is not worth sizing).
    busy = Set{Int}()
    queued = TaskRow[]
    idx = Int[]
    limit = 4 * sum(n.cores for n in pool.nodes) + 64
    lock(table.lock) do
        for (i, r) in enumerate(table.rows)
            if r.state === :running
                push!(busy, r.worker)
            elseif r.state === :todo && length(queued) < limit
                push!(queued, r)
                push!(idx, i)
            end
        end
    end
    for (pid, w) in pool.workers
        pid in busy && (w.last_busy = now)
    end

    cap_c = maximum(n.cores for n in pool.nodes)
    cap_m = maximum(n.mem_gb for n in pool.nodes)
    needs = KeyReq[]
    rows = TaskRow[]
    for (i, r) in zip(idx, queued)
        need = _pool_need(pool, r, opts.deadline, min_time)
        if need.cores > cap_c || need.mem_gb > cap_m
            # Reported now, not retried forever.
            if !(r.kstr in pool.too_big)
                push!(pool.too_big, r.kstr)
                log_event(
                    log,
                    :key_too_big;
                    level=:warn,
                    stage=stage,
                    key=r.kstr,
                    cores=need.cores,
                    mem_gb=round(need.mem_gb; digits=2),
                    node_cores=cap_c,
                    node_mem_gb=round(cap_m; digits=2),
                )
            end
            settle!(table, i, :error)
            continue
        end
        push!(needs, need)
        push!(rows, r)
    end

    idle = [w.size for (pid, w) in pool.workers if !(pid in busy) && !w.retiring]
    starting = KeyReq[]
    for (_, size, n) in values(pool.starting)
        append!(starting, fill(size, n))
    end
    waited = [now - get!(pool.waiting, r.kstr, now) for r in rows]
    have = length(pool.workers) + length(starting)
    plan = plan_spawns(
        needs,
        waited,
        pool.nodes,
        pool.free_c,
        pool.free_m,
        vcat(idle, starting);
        threads=_pool_threads(pool),
        max_threads=pool.max_threads,
        starve_after=pool.starve_after,
        room=max(pool.max_workers - have, 0),
    )
    blocked = Set(rows[i].kstr for i in plan.blocked)
    filter!(kv -> kv[1] in blocked, pool.waiting)

    # One start per (node, size), so the workers of a batch connect concurrently.
    batches = Dict{Tuple{String,KeyReq},Int}()
    for (node, size) in plan.starts
        batches[(node, size)] = get(batches, (node, size), 0) + 1
        pool.free_c[node] -= size.cores
        pool.free_m[node] -= size.mem_gb
    end
    for ((node, size), n) in batches
        tok = (pool.seq += 1)
        pool.starting[tok] = (node, size, n)
        @async _pool_start!(pool, tok, log, stage)
    end
    isempty(batches) || note_workers!(;
        planned=length(pool.workers) + sum(x -> x[3], values(pool.starting); init=0)
    )

    # A size with nothing to do gives its room back when another is waiting for it.
    if !isempty(plan.blocked)
        for (pid, w) in collect(pool.workers)
            (pid in busy || w.retiring) && continue
            now - w.last_busy >= pool.retire_after || continue
            any(n -> _fits(w.size, n), needs) && continue
            w.retiring = true
            push!(master.ctl.retired, pid)
            log_event(
                log,
                :pool_retire;
                stage=stage,
                worker=pid,
                node=w.node,
                cores=w.size.cores,
                mem_gb=round(w.size.mem_gb; digits=2),
            )
            @async begin
                try
                    rmprocs(pid; waitfor=30)
                catch
                end
                _pool_free!(pool, pid)
            end
        end
    end

    # Queued keys, nothing running or starting that could take them, and nothing to start:
    # waiting would not change that.
    pool.stuck =
        !isempty(needs) &&
        isempty(plan.starts) &&
        isempty(pool.starting) &&
        isempty(busy) &&
        !any(w -> any(n -> _fits(w.size, n), needs), values(pool.workers))
    return nothing
end

function _pool_start!(pool::SizedPool, tok::Int, log::EventLog, stage::Symbol)
    node, size, n = pool.starting[tok]
    ids = try
        start_workers(pool.spawner, node, size, n; exeflags=pool.exeflags)
    catch e
        e isa InterruptException && rethrow()
        log_event(
            log,
            :pool_spawn_failed;
            level=:warn,
            stage=stage,
            node=node,
            cores=size.cores,
            mem_gb=round(size.mem_gb; digits=2),
            n=n,
            err=_short_err(e),
        )
        Int[]
    end
    delete!(pool.starting, tok)
    for pid in ids
        pool.workers[pid] = PoolWorker(node, size, time(), false)
    end
    missing_n = n - length(ids)
    if missing_n > 0
        pool.free_c[node] += missing_n * size.cores
        pool.free_m[node] += missing_n * size.mem_gb
        pool.fails += 1
    else
        pool.fails = 0
    end
    isempty(ids) || log_event(
        log,
        :pool_spawn;
        stage=stage,
        node=node,
        cores=size.cores,
        mem_gb=round(size.mem_gb; digits=2),
        n=length(ids),
    )
    note_workers!(; launched=length(pool.workers))
    return nothing
end

# Is there still something the pool is working towards? Ten failed starts in a row, or a queue
# nothing can take, end the wait.
function _pool_wants(pool::SizedPool, table::TaskTable)
    pool.fails >= 10 && return false
    isempty(pool.starting) || return true
    return _has_queued(table) && !pool.stuck
end

"""
    pool_summary(pool) -> Vector{NamedTuple}

The pool's workers by size: `(; cores, mem_gb, workers)`, largest first.
"""
function pool_summary(pool::SizedPool)
    counts = Dict{KeyReq,Int}()
    for w in values(pool.workers)
        counts[w.size] = get(counts, w.size, 0) + 1
    end
    rows = [(; cores=s.cores, mem_gb=s.mem_gb, workers=n) for (s, n) in counts]
    return sort!(rows; by=r -> (-r.cores, -r.mem_gb))
end

"""
    shutdown!(pool)

Remove every worker the pool started and give their room back.
"""
function shutdown!(pool::SizedPool)
    ids = [p for p in keys(pool.workers) if p in procs()]
    try
        isempty(ids) || rmprocs(ids; waitfor=30)
    catch
    end
    foreach(p -> _pool_free!(pool, p), collect(keys(pool.workers)))
    return nothing
end

export KeyReq, PoolNode, Spawner, LocalSpawner, SlurmStepSpawner, SizedPool
export worker_size, plan_spawns, pool_summary, default_spawner
