# Pool — workers sized to the keys they run.
#
# `init_workers!` starts `n` identical workers and `run!` hands any key to any of them, so a sweep
# whose keys differ in size has to size every worker for its largest key. The figures that
# prompted this are FiniteTemperature.jl's (128 cores, 226 GB per node; keys from 1 core / 2 GB to
# 7 cores / 12 GB: ~55 small workers on a node sized per key, 18 sized for the largest). They are
# that campaign's, reported in #70; nothing in this repository reproduces them.
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
# they need an allocation to run and have not been run from this package. Where a default or a
# check below exists because of a failure on a cluster, the comment says which.

using Distributed
using LinearAlgebra: BLAS

"""
    KeyReq(cores, mem_gb)

What one key needs from the worker that runs it, and the size a worker was started with. `cores`
is at least 1 and `mem_gb` positive and finite: a zero or negative request would fit any worker
and give room back to a node when it is placed.
"""
struct KeyReq
    cores::Int
    mem_gb::Float64
    function KeyReq(cores::Integer, mem_gb::Real)
        cores >= 1 || throw(ArgumentError("KeyReq: cores must be >= 1, got $cores"))
        (isfinite(mem_gb) && mem_gb > 0) ||
            throw(ArgumentError("KeyReq: mem_gb must be positive and finite, got $mem_gb"))
        return new(Int(cores), Float64(mem_gb))
    end
end

# Can a worker of size `have` run a key that needs `need`?
_fits(have::KeyReq, need::KeyReq) = need.cores <= have.cores && need.mem_gb <= have.mem_gb

"""
    PoolNode(name, cores, mem_gb)

A node workers can be started on, and what it offers. A node with no cores or no memory left for
workers (`0`) is allowed — it simply takes none.
"""
struct PoolNode
    name::String
    cores::Int
    mem_gb::Float64
    function PoolNode(name::AbstractString, cores::Integer, mem_gb::Real)
        cores >= 0 || throw(ArgumentError("PoolNode $name: cores must be >= 0, got $cores"))
        (isfinite(mem_gb) && mem_gb >= 0) || throw(
            ArgumentError("PoolNode $name: mem_gb must be >= 0 and finite, got $mem_gb")
        )
        return new(String(name), Int(cores), Float64(mem_gb))
    end
end

"""
    Spawner

Where a [`SizedPool`](@ref)'s workers come from. A backend implements `pool_nodes(s)` (the
[`PoolNode`](@ref)s it offers) and `start_workers(s, node, size, n; exeflags)` (start `n` workers
of [`KeyReq`](@ref) `size` on `node`, return the Distributed ids of the ones that are ready).
[`LocalSpawner`](@ref) and [`SlurmStepSpawner`](@ref) are provided.
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
    SlurmStepSpawner(; nodes=<env or the allocation>, mem_per_node_mb=<env or the allocation>,
                     master_gb=3.0, srun_gb=0.008, headroom_gb=4.0)

Workers are job steps of the current allocation: `srun --exact -N1 -n1 --nodelist=<node>
--cpus-per-task=<cores> --mem=<mem>`, so each worker is its own step with its own memory cgroup.
Every node offers its CPUs and memory, less `headroom_gb`. The master's node also keeps one core
and `master_gb + srun_gb × (cores of this master's nodes)`: every worker is an `srun` client
process on the master's node, a few MB each, and a large job has thousands.

`nodes` limits it to part of the allocation — a master of a node group
([`split_nodes`](@ref)); it defaults to `ENV["SWEEPRUNNER_NODELIST"]`. A master that is itself a
job step sees the STEP's `--mem` in `SLURM_MEM_PER_NODE`, not the node's: pass
`mem_per_node_mb`, or set `SWEEPRUNNER_MEM_PER_NODE_MB`, to the allocation's figure.

Each default is what the downstream pool arrived at after a failure on a cluster: 4 GB of
headroom after workers were killed at the node's limit, the `srun` reservation after the master's
node ran short on a 72-node job.
"""
struct SlurmStepSpawner <: Spawner
    nodes::Vector{PoolNode}
end

# `128(x2),64` -> [128, 128, 64]
function _expand_slurm_counts(s::AbstractString, n::Int)
    out = _slurm_cpus_per_node(s)
    out === nothing && error("SLURM_JOB_CPUS_PER_NODE: cannot read $(repr(s))")
    length(out) == n || error("SLURM_JOB_CPUS_PER_NODE $(repr(s)) does not cover $n nodes")
    return out
end

# The allocation's nodes with what each offers, from the environment a Slurm job has.
function _slurm_pool_nodes(
    env::AbstractDict,
    me::AbstractString;
    only=nothing,
    master_gb::Real,
    headroom_gb::Real,
    srun_gb::Real=0.0,
    mem_per_node_mb=nothing,
)
    names = expand_nodelist(env["SLURM_JOB_NODELIST"])
    isempty(names) &&
        error("SLURM_JOB_NODELIST $(repr(env["SLURM_JOB_NODELIST"])) not read")
    cpus = _expand_slurm_counts(get(env, "SLURM_JOB_CPUS_PER_NODE", ""), length(names))
    mem_mb = if mem_per_node_mb !== nothing
        fill(Float64(mem_per_node_mb), length(names))
    elseif haskey(env, "SLURM_MEM_PER_NODE")
        fill(parse(Float64, env["SLURM_MEM_PER_NODE"]), length(names))
    elseif haskey(env, "SLURM_MEM_PER_CPU")
        parse(Float64, env["SLURM_MEM_PER_CPU"]) .* cpus
    else
        error("neither SLURM_MEM_PER_NODE nor SLURM_MEM_PER_CPU is set: node memory unknown")
    end
    keep = [only === nothing || n in only for n in names]
    any(keep) || error("none of the nodes asked for is in the allocation")
    # The srun clients of every worker this master can have live on the master's node.
    reserve = master_gb + srun_gb * sum(cpus[keep])
    nodes = PoolNode[]
    for (n, c, m) in zip(names[keep], cpus[keep], mem_mb[keep])
        master = n == me || startswith(me, n * ".")
        push!(
            nodes,
            PoolNode(
                n,
                max(c - (master ? 1 : 0), 0),
                max(0.0, m / 1024 - headroom_gb - (master ? reserve : 0.0)),
            ),
        )
    end
    return nodes
end

function SlurmStepSpawner(;
    nodes=get(ENV, "SWEEPRUNNER_NODELIST", nothing),
    mem_per_node_mb=tryparse(Float64, get(ENV, "SWEEPRUNNER_MEM_PER_NODE_MB", "")),
    master_gb::Real=3.0,
    srun_gb::Real=0.008,
    headroom_gb::Real=4.0,
)
    only = if nodes === nothing
        nothing
    elseif nodes isa AbstractString
        Set(expand_nodelist(nodes))
    else
        Set(String.(nodes))
    end
    return SlurmStepSpawner(
        _slurm_pool_nodes(
            ENV, gethostname(); only, master_gb, headroom_gb, srun_gb, mem_per_node_mb
        ),
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
# its host name resolves to the one every node can.
function _bind_flag(node::AbstractString)
    ip = try
        Distributed.Sockets.getaddrinfo(node, Distributed.Sockets.IPv4)
    catch e
        # Said, because the failure this guards against (workers on another rack that never
        # connect) would otherwise come back without a word.
        @warn "SweepRunner: node name does not resolve; its workers bind to their default address, which a master on another rack may not reach" node exception =
            e maxlog = 5
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

Start `n` workers of `size` on `node` and return the Distributed ids of the ones that are ready,
each with its BLAS threads set to `size.cores`. Fewer ids than `n` means the rest did not start;
a worker that started but could not be readied is removed again, so every id returned is usable
and nothing else is left running.
"""
function start_workers(
    s::Union{LocalSpawner,SlurmStepSpawner},
    node::AbstractString,
    size::KeyReq,
    n::Integer;
    exeflags,
)
    ids = addprocs(
        StepManager(String(node), size.cores, size.mem_gb, s isa SlurmStepSpawner, Int(n));
        exeflags=exeflags,
    )
    return _ready_workers!(ids, size)
end

_set_blas_threads(n::Integer) = (BLAS.set_num_threads(n); nothing)

# Ready each started worker by itself: one that fails is removed and fails alone, not its batch.
function _ready_workers!(ids::AbstractVector{<:Integer}, size::KeyReq; ready=_ready_one)
    good = Int[]
    @sync for w in ids
        @async try
            ready(w, size)
            push!(good, w)
        catch e
            e isa InterruptException && rethrow()
            _kill_worker!(w)
        end
    end
    return sort!(good)
end

function _ready_one(w::Integer, size::KeyReq)
    # The package first: a fresh worker has loaded nothing, and cannot even be told what to run.
    Distributed.remotecall_eval(Main, [w], :(using SweepRunner))
    remotecall_fetch(_set_blas_threads, w, size.cores)
    return nothing
end

# Worker `id`'s launching process: its `srun` client, or the local `julia --worker`. `nothing`
# for a worker started some other way.
function _launcher(id::Integer)
    p = try
        Distributed.worker_from_id(id).config.process
    catch
        return nothing
    end
    return p === nothing ? nothing : something(p)
end

# Whether worker `id` is gone: out of `procs()`, or its launching process has exited. The latter
# is seen even when Distributed has not noticed the connection drop — downstream, a step killed
# for memory under a running key left the master's remote call waiting for minutes, the worker
# still counted busy.
function _worker_gone(id::Integer, live)::Bool
    id in live || return true
    p = _launcher(id)
    return p isa Base.Process && process_exited(p)
end

# Remove worker `id` for certain. Asked to leave first, which an idle worker does at once and
# cleanly; one that does not (it is inside a key, or it is hung) has what launched it killed —
# a job step dies with its srun client.
function _kill_worker!(id::Integer; waitfor::Real=5)
    p = _launcher(id)
    try
        id in procs() && rmprocs(id; waitfor=waitfor)
    catch
    end
    try
        if p isa Base.Process && process_running(p)
            kill(p)
            timedwait(() -> !process_running(p), 5.0) === :ok || kill(p, Base.SIGKILL)
        end
    catch
    end
    return nothing
end

# ── the planner ─────────────────────────────────────────────────────────────────────────────────

"""
    worker_size(need, node, free_cores, free_mem; threads=:throughput, max_threads=8) -> KeyReq

The size a worker is started with for a key that needs `need`, on a node with that much room.

- `:throughput` — the cores its memory stands for on this node
  (`need.mem_gb × free cores / free memory`), at least what the key declares and at most
  `max_threads`. Where memory binds before cores, a key sized by its cores alone strands the
  rest of the node; this hands those cores to the keys whose memory holds them.
- `:fastest` — a key that declares more than one core gets up to `max_threads`, and the memory
  that comes with them on this node; a one-core key is sized as under `:throughput`. For
  allocations where cores are not the scarce thing, or a key that has to finish. (A key that
  says it is serial is left serial: threads would go where the key cannot use them.)
"""
function worker_size(
    need::KeyReq,
    node::PoolNode,
    free_c::Integer,
    free_m::Real;
    threads::Symbol=:throughput,
    max_threads::Integer=8,
)
    if threads === :fastest && need.cores > 1
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
                starve_after, room) -> (; starts, blocked, capped)

Which workers to start. `needs` are the queued keys' requirements in queue order and `waited`
how long each has found no room; `covering` are the sizes of the workers that are idle or already
starting, each of which will take one key it fits. `nodes` are the nodes that may be used: leave
a drained node out.

In order: a need that a covering worker fits takes it; otherwise the worker is started on the
node that keeps the most memory free after it; a need that fits nowhere is `blocked`. Smaller
needs behind a blocked one still start (backfill) until it has waited `starve_after` seconds:
from then on nothing is started ahead of it, so the room it needs is freed by keys finishing.
At most `room` workers are planned; `capped` says a start was wanted past that.

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
    capped = false
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
        if length(starts) >= room
            capped = true
            break
        end
        push!(starts, (best, size))
        fc[best] -= size.cores
        fm[best] -= size.mem_gb
    end
    return (; starts, blocked, capped)
end

# ── the pool ────────────────────────────────────────────────────────────────────────────────────

mutable struct PoolWorker
    const node::String
    const size::KeyReq
    last_busy::Float64
    retiring::Bool
end

# How many starts may fail in a row before the pool stops trying and says so.
const _POOL_MAX_FAILS = 10

# The most workers one master holds under Slurm when the cluster's limit cannot be read: each
# worker is an srun client on the master's node. Downstream ran at 1500 after a master that
# planned 3735 stalled, without a message, at 1782.
const _POOL_SLURM_CAP = 1500

"""
    SizedPool(spawner=default_spawner(); key_req, threads=:throughput, max_threads=8,
              speedup=(key, cores) -> 1.0, retire_after=120.0, starve_after=600.0,
              stall_after=600.0, mem_growth=1.5, max_workers=<limit>, poll=1.0, keep=false,
              exeflags=<project, -t1>)

A pool of workers sized to the keys they run. Give it to [`run!`](@ref) / [`run_loop!`](@ref) as
`pool=`; it starts workers as the queue needs them (no `init_workers!`). Its workers are removed
when the `run!` / `run_loop!` it was given to returns, unless `keep=true` (then call
[`shutdown!`](@ref) yourself).

- `key_req` — `key -> KeyReq(cores, mem_gb)`: what a key needs.
- `threads` — how many cores a worker is given beyond what its key declares
  ([`worker_size`](@ref)): `:throughput` (the cores its memory stands for; most work per
  node-hour), `:fastest` (up to `max_threads` for keys that declare more than one core;
  time-to-solution), or `:finish_by` (as `:throughput`, but a key that would not reach its next
  checkpoint before the job's `deadline` is given the cores that get it there, by `speedup`).
- `speedup` — `(key, cores) -> factor` relative to one core, for `:finish_by`
  ([`measured_speedup`](@ref) builds one from the cost records).
- `retire_after` — an idle worker whose size no queued key fits is retired after this long, when
  another size is waiting for room.
- `starve_after` — how long a key that fits nowhere lets smaller keys start ahead of it.
- `stall_after` — starts that neither join nor fail for this long are said out loud
  (`pool_stalled`), naming the usual cause under Slurm.
- `mem_growth` — a key whose worker died is retried with this much more memory (up to what a
  node has), at most the dispatcher's death bound times. Must be > 1.
- `max_workers` — the most workers this pool holds at once. Under Slurm it defaults to the
  per-master limit ([`srun_worker_limit`](@ref), else 1500): past it workers neither join nor
  fail. Reaching it is logged once (`pool_at_limit`).

After ten failed starts in a row the pool gives up and `run!` throws: a pool that cannot start
workers is not a round that ended.
"""
mutable struct SizedPool
    const spawner::Spawner
    const key_req::Any
    const threads::Symbol
    const max_threads::Int
    const speedup::Any
    const retire_after::Float64
    const starve_after::Float64
    const stall_after::Float64
    const mem_growth::Float64
    const max_workers::Int
    const limit_source::String
    const poll::Float64
    const keep::Bool
    const exeflags::Cmd
    const nodes::Vector{PoolNode}
    const free_c::Dict{String,Int}
    const free_m::Dict{String,Float64}
    const workers::Dict{Int,PoolWorker}
    const starting::Dict{Int,Tuple{String,KeyReq,Int}}     # token => (node, size, how many)
    const memreq::Dict{String,Float64}                     # raised after a worker died on it
    const waiting::Dict{String,Float64}                    # since when a key has found no room
    const too_big::Set{String}
    # Workers that existed before the pool started any: not its own, and take any key.
    const foreign::Set{Int}
    seq::Int
    fails::Int
    stuck::Bool
    snapshot::Bool          # `foreign` has been taken
    last_join::Float64      # the last time a start succeeded or failed
    last_stall::Float64
    said_limit::Bool
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

# The cap on workers and where it came from. Under Slurm an unreadable limit is a default cap,
# not no cap: the failure it prevents is silent.
function _pool_limit(spawner::Spawner, max_workers; limit=srun_worker_limit)
    max_workers === nothing || return (Int(max_workers), "max_workers")
    spawner isa SlurmStepSpawner || return (typemax(Int), "none")
    n = tryparse(Int, get(ENV, "SWEEPRUNNER_MAX_WORKERS", ""))
    n === nothing || return (n, "SWEEPRUNNER_MAX_WORKERS")
    lim = limit()
    lim === nothing || return (lim, "SrunPortRange")
    return (_POOL_SLURM_CAP, "default (SrunPortRange could not be read)")
end

function SizedPool(
    spawner::Spawner=default_spawner();
    key_req,
    threads::Symbol=:throughput,
    max_threads::Integer=8,
    speedup=(key, cores) -> 1.0,
    retire_after::Real=120.0,
    starve_after::Real=600.0,
    stall_after::Real=600.0,
    mem_growth::Real=1.5,
    max_workers::Union{Integer,Nothing}=nothing,
    poll::Real=1.0,
    keep::Bool=false,
    exeflags::Cmd=_default_exeflags(),
)
    threads in (:throughput, :fastest, :finish_by) || throw(
        ArgumentError(
            "SizedPool: threads must be :throughput, :fastest or :finish_by, got " *
            repr(threads),
        ),
    )
    max_threads >= 1 || throw(ArgumentError("SizedPool: max_threads must be >= 1"))
    mem_growth > 1 || throw(
        ArgumentError(
            "SizedPool: mem_growth must be > 1 (a key whose worker died would be retried " *
            "with the same memory), got $mem_growth",
        ),
    )
    poll > 0 || throw(ArgumentError("SizedPool: poll must be > 0, got $poll"))
    (retire_after >= 0 && starve_after >= 0 && stall_after >= 0) || throw(
        ArgumentError("SizedPool: retire_after, starve_after and stall_after must be >= 0"),
    )
    limit, source = _pool_limit(spawner, max_workers)
    limit >= 1 || throw(ArgumentError("SizedPool: max_workers must be >= 1, got $limit"))
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
        Float64(stall_after),
        Float64(mem_growth),
        limit,
        source,
        Float64(poll),
        keep,
        exeflags,
        nodes,
        Dict(n.name => n.cores for n in nodes),
        Dict(n.name => n.mem_gb for n in nodes),
        Dict{Int,PoolWorker}(),
        Dict{Int,Tuple{String,KeyReq,Int}}(),
        Dict{String,Float64}(),
        Dict{String,Float64}(),
        Set{String}(),
        Set{Int}(),
        0,
        0,
        false,
        false,
        time(),
        0.0,
        false,
    )
end

# What `key` needs now: its declared size, the memory raised after a death, and under
# `:finish_by` the cores that get it to its next checkpoint before the deadline.
function _pool_need(pool::SizedPool, key::DataKey, kstr::AbstractString, deadline, min_time)
    base = pool.key_req(key)::KeyReq
    base = KeyReq(base.cores, max(base.mem_gb, get(pool.memreq, kstr, 0.0)))
    (pool.threads === :finish_by && deadline !== nothing && min_time !== nothing) ||
        return base
    left = deadline - time()
    need = something(key_seconds(min_time, key), 0.0)
    s0 = max(Float64(pool.speedup(key, base.cores)), 1e-9)
    for c in base.cores:max(pool.max_threads, base.cores)
        need * s0 / max(Float64(pool.speedup(key, c)), 1e-9) <= left &&
            return KeyReq(c, base.mem_gb)
    end
    return KeyReq(max(pool.max_threads, base.cores), base.mem_gb)
end

function _pool_need(pool::SizedPool, row::TaskRow, deadline, min_time)
    return _pool_need(pool, row.key, row.kstr, deadline, min_time)
end

# How long `key` needs to get somewhere AT THE SIZE THE POOL WILL RUN IT WITH. The deadline
# hold-back asks this, so a key that `:finish_by` gave more cores to is not then refused for the
# time it would have taken on fewer.
function _pool_min_time(pool::SizedPool, key::DataKey, deadline, min_time)::Float64
    need = key_seconds(min_time, key)
    # Unknown stays unknown (NaN): `_fits` decides what that means, not this function.
    need === nothing && return NaN
    pool.threads === :finish_by || return need
    base = pool.key_req(key)::KeyReq
    size = _pool_need(pool, key, canonical(key), deadline, min_time)
    s0 = max(Float64(pool.speedup(key, base.cores)), 1e-9)
    return need * s0 / max(Float64(pool.speedup(key, size.cores)), 1e-9)
end

# May worker `pid` take `row`? A worker the pool started takes what its size holds; one that was
# there before the pool takes anything, as it did without one.
function _pool_accepts(pool::SizedPool, pid::Int, row::TaskRow, deadline, min_time)::Bool
    w = get(pool.workers, pid, nothing)
    w === nothing && return pid in pool.foreign
    w.retiring && return false
    return _fits(w.size, _pool_need(pool, row, deadline, min_time))
end

# May the dispatcher give worker `pid` a dispatch task yet? Not between the moment `addprocs`
# makes it visible and the moment the pool knows its size: in that window it would be taken for
# a worker that holds anything.
function _pool_adoptable(pool::SizedPool, pid::Int)
    return haskey(pool.workers, pid) || pid in pool.foreign
end

function _pool_retiring(pool::SizedPool, pid::Int)
    w = get(pool.workers, pid, nothing)
    return w !== nothing && w.retiring
end

_pool_threads(pool::SizedPool) = pool.threads === :fastest ? :fastest : :throughput

# A worker died under `row`: most often its memory. The key comes back asking for more than the
# most it has had, whichever of the worker's size, an earlier raise, or its declaration that is.
function _pool_death!(pool::SizedPool, row::TaskRow, pid::Int, log::EventLog, stage::Symbol)
    w = get(pool.workers, pid, nothing)
    had = max(
        pool.key_req(row.key).mem_gb,
        get(pool.memreq, row.kstr, 0.0),
        w === nothing ? 0.0 : w.size.mem_gb,
    )
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

# Give a worker's room back and forget it. The only place room comes back for a worker.
function _pool_free!(pool::SizedPool, pid::Int)
    w = pop!(pool.workers, pid, nothing)
    w === nothing && return nothing
    pool.free_c[w.node] += w.size.cores
    pool.free_m[w.node] += w.size.mem_gb
    return nothing
end

# Take a worker out: no more keys, the process removed, its room back once it is gone. The pool
# is the one owner of this; the dispatch task only sees `retiring` and leaves.
function _pool_retire!(pool::SizedPool, pid::Int)
    w = get(pool.workers, pid, nothing)
    (w === nothing || w.retiring) && return nothing
    w.retiring = true
    @async begin
        _kill_worker!(pid)
        _pool_free!(pool, pid)
    end
    return nothing
end

"""
    _pool_tick!(pool, table, master, log, stage, opts, min_time; fits)

One pass of the pool: forget workers that are gone, report keys no node can hold, start the
workers the queue needs ([`plan_spawns`](@ref)), and retire idle workers whose size is no longer
wanted while another size waits for room.

It follows what the master was told: nothing is started while the master is paused or stopping,
a drained node offers no room and its workers no cover, a `:resize` target caps the pool, and a
key the deadline will hold back (`fits`) is not planned for.
"""
function _pool_tick!(
    pool::SizedPool,
    table::TaskTable,
    master::Master,
    log::EventLog,
    stage::Symbol,
    opts::RunOpts,
    min_time;
    fits=Returns(true),
)
    now = time()
    c = master.ctl
    if !pool.snapshot
        # What was here before the pool started anything is not the pool's.
        pool.snapshot = true
        union!(pool.foreign, p for p in workers() if p != myid())
        log_event(
            log,
            :pool_limit;
            stage=stage,
            max_workers=pool.max_workers == typemax(Int) ? nothing : pool.max_workers,
            source=pool.limit_source,
        )
    end
    live = Set(procs())
    for pid in collect(keys(pool.workers))
        _worker_gone(pid, live) || continue
        # Its launcher exited: make Distributed notice, so the call waiting on it returns.
        pid in live && @async _kill_worker!(pid)
        _pool_free!(pool, pid)
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
    _pool_report!(pool)

    # Paused or stopping: the queue is not to be drawn, so nothing is started for it.
    if c.paused || _stop_reason(opts, master) !== nothing
        pool.stuck = false
        return nothing
    end

    cap_c = maximum(n.cores for n in pool.nodes)
    cap_m = maximum(n.mem_gb for n in pool.nodes)
    needs = KeyReq[]
    rows = TaskRow[]
    for (i, r) in zip(idx, queued)
        # No worker is started for a key the deadline holds back. It is settled here: with a
        # pool no worker of its size may ever exist to draw it and pass it over, and left
        # queued it kept the round waiting until the deadline itself. The time left only
        # shrinks, so a key that does not fit now will not fit later in this round.
        if !fits(r.key)
            settle!(table, i, :no_fit)
            continue
        end
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

    usable(node) = !(node in c.drained)
    idle = [
        w.size for
        (pid, w) in pool.workers if !(pid in busy) && !w.retiring && usable(w.node)
    ]
    starting = KeyReq[]
    for (node, size, n) in values(pool.starting)
        usable(node) && append!(starting, fill(size, n))
    end
    waited = [now - get!(pool.waiting, r.kstr, now) for r in rows]
    have = length(pool.workers) + sum(x -> x[3], values(pool.starting); init=0)
    cap = c.target === nothing ? pool.max_workers : min(pool.max_workers, c.target)
    plan = plan_spawns(
        needs,
        waited,
        [n for n in pool.nodes if usable(n.name)],
        pool.free_c,
        pool.free_m,
        vcat(idle, starting);
        threads=_pool_threads(pool),
        max_threads=pool.max_threads,
        starve_after=pool.starve_after,
        room=max(cap - have, 0),
    )
    blocked = Set(rows[i].kstr for i in plan.blocked)
    filter!(kv -> kv[1] in blocked, pool.waiting)
    if plan.capped && !pool.said_limit
        pool.said_limit = true
        log_event(
            log,
            :pool_at_limit;
            level=:warn,
            stage=stage,
            held=have,
            max_workers=cap,
            queued=length(needs),
            source=c.target === nothing ? pool.limit_source : "resize",
        )
    end

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
    isempty(batches) || _pool_report!(pool)

    # A size with nothing to do gives its room back when another is waiting for it: for the
    # node's cores and memory, or for a place under the worker limit.
    waiting_for_room = !isempty(plan.blocked) || plan.capped
    # Idle workers no queued key fits: what retiring can still free.
    unwanted = Int[
        pid for (pid, w) in pool.workers if
        !(pid in busy) && !w.retiring && !any(n -> _fits(w.size, n), needs)
    ]
    if waiting_for_room
        for pid in unwanted
            w = pool.workers[pid]
            now - w.last_busy >= pool.retire_after || continue
            log_event(
                log,
                :pool_retire;
                stage=stage,
                worker=pid,
                node=w.node,
                cores=w.size.cores,
                mem_gb=round(w.size.mem_gb; digits=2),
            )
            _pool_retire!(pool, pid)
        end
    end

    # Starts that neither join nor fail. Under Slurm the usual cause is the srun port range of
    # the master's node, and it comes without an error.
    if !isempty(pool.starting) &&
        now - pool.last_join >= pool.stall_after &&
        now - pool.last_stall >= pool.stall_after
        pool.last_stall = now
        log_event(
            log,
            :pool_stalled;
            level=:warn,
            stage=stage,
            starting=sum(x -> x[3], values(pool.starting); init=0),
            joined=length(pool.workers),
            secs=round(Int, now - pool.last_join),
            hint="under Slurm: the master node's srun port range " *
                 "(scontrol show config | grep SrunPortRange)",
        )
    end

    # Queued keys, nothing running or starting that could take them, and nothing to start:
    # waiting would not change that. It WOULD while a worker is on its way out or is about to
    # be retired for the room: the keys behind it start when it is gone. Without that the round
    # ended the moment the small keys were done, with the large ones reported `worker_lost`.
    freeing =
        any(w -> w.retiring, values(pool.workers)) ||
        (waiting_for_room && !isempty(unwanted))
    pool.stuck =
        !isempty(needs) &&
        isempty(plan.starts) &&
        isempty(pool.starting) &&
        isempty(busy) &&
        !freeing &&
        !any(w -> !w.retiring && any(n -> _fits(w.size, n), needs), values(pool.workers))
    return nothing
end

# What the status compares the workers that joined against: the pool's current plan, and the
# processes it has asked for (a start that is under way has been launched and has not joined).
function _pool_report!(pool::SizedPool)
    n = length(pool.workers) + sum(x -> x[3], values(pool.starting); init=0)
    note_workers!(; planned=n + length(pool.foreign), launched=n + length(pool.foreign))
    return nothing
end

function _pool_start!(pool::SizedPool, tok::Int, log::EventLog, stage::Symbol)
    node, size, n = pool.starting[tok]
    ids = Int[]
    try
        ids = start_workers(pool.spawner, node, size, n; exeflags=pool.exeflags)
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
    finally
        # Whatever happened, the start is over: its workers are registered or its room is back.
        # Left set, the pool would wait on it for ever.
        for pid in ids
            pool.workers[pid] = PoolWorker(node, size, time(), false)
        end
        delete!(pool.starting, tok)
        short = n - length(ids)
        pool.free_c[node] += short * size.cores
        pool.free_m[node] += short * size.mem_gb
        pool.last_join = time()
        if short > 0
            pool.fails += 1
            isempty(ids) || log_event(
                log,
                :pool_spawn_short;
                level=:warn,
                stage=stage,
                node=node,
                cores=size.cores,
                asked=n,
                started=length(ids),
            )
        else
            pool.fails = 0
        end
        _pool_report!(pool)
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
    return nothing
end

# Has the pool stopped trying? Too many starts failed in a row.
_pool_gave_up(pool::SizedPool) = pool.fails >= _POOL_MAX_FAILS

# Is there still something the pool is working towards?
function _pool_wants(pool::SizedPool, table::TaskTable)
    _pool_gave_up(pool) && return false
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

Remove every worker the pool started and give their room back. `run!` / `run_loop!` call it when
they return, unless the pool was made with `keep=true`.
"""
function shutdown!(pool::SizedPool)
    for pid in collect(keys(pool.workers))
        _kill_worker!(pid)
        _pool_free!(pool, pid)
    end
    note_workers!(; planned=0, launched=0)
    return nothing
end

export KeyReq, PoolNode, Spawner, LocalSpawner, SlurmStepSpawner, SizedPool
export worker_size, plan_spawns, pool_summary, default_spawner
