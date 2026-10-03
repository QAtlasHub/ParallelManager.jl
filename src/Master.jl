# Master — what one `run!` / `run_loop!` invocation knows about itself and its workers.
#
# A master is the process that called `run!`. Several of them can share a vault (one per batch
# job), so everything a master publishes lives under its own directory and nothing here is a
# singleton.

using Distributed
using DataVault: Vault

"""
    state_root(vault) -> String

Where SweepRunner keeps its own state for one `(project, run)`:

    <vault.outdir>/sweeprunner/<project_name>/<vault.run>/

Pure function; does not touch the filesystem.
"""
function state_root(vault::Vault)
    return joinpath(vault.outdir, "sweeprunner", vault.spec.study.project_name, vault.run)
end

"""
    WorkerSample

The master's last reading of one worker: cumulative `cpu` seconds, `rss` bytes, `wall` (the
worker's `time()` at the reading) and `util`, the CPU time used between this reading and the one
before it, divided by the wall time between them and by the worker's cores (`NaN` until there are
two readings).

A worker inside a `work_fn` that does not yield answers when it next does, so `wall` can be older
than the status that carries it; the status says how old.
"""
struct WorkerSample
    cpu::Float64
    wall::Float64
    rss::Int
    util::Float64
end

const WorkerIdentity = @NamedTuple{host::String, pid::Int, cores::Int}

"""
    Master()

One master: its identity, what it has learned about its workers, and the round it is running.
[`run_loop!`](@ref) builds one and keeps it across its rounds; a bare [`run!`](@ref) builds its
own.

- `id` — `<hostname>_<pid>`, the same pair that names the master's event log.
- `job` — the scheduler's id for the job this master runs in (`""` outside one).
- `started` — `time()` at construction.
- `who` — Distributed id => `(; host, pid, cores)` of each worker asked so far.
- `samples` — Distributed id => the last [`WorkerSample`](@ref).
- `table` — the [`TaskTable`](@ref) of the round in progress (or of the last one).
- `state` — `:starting`, `:running`, `:waiting` (between rounds of a `run_loop!`), `:ended`.

The rest is bookkeeping for the status file (see `Status.jl`).
"""
mutable struct Master
    const id::String
    const host::String
    const pid::Int
    const job::String
    const started::Float64
    const who::Dict{Int,WorkerIdentity}
    const samples::Dict{Int,WorkerSample}
    const probing::Set{Int}
    const progress::Dict{String,Progress}
    const warnings::Vector{String}
    const lock::ReentrantLock
    table::Union{TaskTable,Nothing}
    vault::Union{Vault,Nothing}
    stage::String
    state::Symbol
    multi::Bool
    interval::Float64
    last_status::Float64
    short_since::Float64
    short_logged::Tuple{Int,Int,Int}
end

function Master()
    return Master(
        string(gethostname(), "_", getpid()),
        gethostname(),
        getpid(),
        _slurm_queue_id(),
        time(),
        Dict{Int,WorkerIdentity}(),
        Dict{Int,WorkerSample}(),
        Set{Int}(),
        Dict{String,Progress}(),
        String[],
        ReentrantLock(),
        nothing,
        nothing,
        "",
        :starting,
        false,
        0.0,
        0.0,
        0.0,
        (-1, -1, -1),
    )
end

_whoami()::WorkerIdentity = (; host=gethostname(), pid=getpid(), cores=_my_cores())

# Ask the workers not asked yet who they are, all at once. A worker that cannot answer is left out,
# and the dispatcher does not hand it work: it could not be named in a lock.
function _identify_workers!(m::Master, pids)
    unknown = lock(() -> [p for p in pids if !haskey(m.who, p)], m.lock)
    isempty(unknown) && return nothing
    answers = asyncmap(unknown) do p
        try
            p == myid() ? _whoami() : remotecall_fetch(_whoami, p)
        catch e
            e isa InterruptException && rethrow()
            nothing
        end
    end
    lock(m.lock) do
        for (p, a) in zip(unknown, answers)
            a === nothing || (m.who[p] = a)
        end
    end
    return nothing
end

export state_root, Master
