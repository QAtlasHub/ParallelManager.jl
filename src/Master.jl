# Master — what one `run!` / `run_loop!` invocation knows about itself and its workers.
#
# A master is the process that called `run!`. Several of them can share a vault (one per batch
# job), so everything a master publishes lives under its own directory and nothing here is a
# singleton.

using Distributed
using DataVault: Vault
using ParamIO: DataKey

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

# The control channel's types live here because a `Master` holds them; what is done with them is
# in `Control.jl`.

struct KeyFilter
    select::Dict{String,Vector{Any}}
    samples::Union{Vector{Int},Nothing}
end

# A running unit that was told to stop: cut once `deadline` passes.
mutable struct StopOrder
    deadline::Float64
    cut::Bool
    request::String
    interrupt::Bool
end

# What a worker needs to tell whether a stop request covers the unit it is on.
mutable struct StopWatch
    const since::Float64          # requests older than this are not for this master
    const master::String
    const job::String
    const seen::Set{String}
    const unread::Dict{String,Int}
    checked::Float64
    hit::Bool
end

function StopWatch(since, master, job)
    return StopWatch(since, master, job, Set{String}(), Dict{String,Int}(), 0.0, false)
end

# A key's checkpoint bookkeeping during one `work_fn` call (Checkpoint.jl). `mode` is `:normal`
# under `run!`; `check_checkpoints` uses `:never` (never due) and `:cut` (always due, and cut
# after every save).
mutable struct CheckpointState
    const mode::Symbol
    last::Float64             # when the key started, then when it was last saved
    saves::Int
    used::Bool
    near::Bool                # the deadline is close
    near_saved::Bool          # ... and a save has been made since it came close
end

function CheckpointState(mode::Symbol=:normal)
    return CheckpointState(mode, time(), 0, false, false, false)
end

"""
    ControlState

What requests have changed about a master, for the rest of its life: whether it is paused or told
to stop, the nodes it no longer dispatches to, the workers it is retiring, the filters it has
cancelled and prioritised, the keys it was given, and the stop orders in force.
"""
mutable struct ControlState
    paused::Bool
    stop_all::Bool
    # Why `stop_all`: `:request` (someone asked) or `:underused` (the master left on purpose).
    stop_why::Symbol
    target::Union{Int,Nothing}
    last_poll::Float64
    # `n -> start n more workers`, given to `run!` as `spawn`; `nothing` when there is none.
    spawn::Any
    const drained::Set{String}
    const retired::Set{Int}
    const seen::Set{String}
    const cancels::Vector{KeyFilter}
    const priorities::Vector{KeyFilter}
    const extra::Vector{DataKey}
    const stopping::Dict{String,StopOrder}
    # Cuts under way (a worker being removed, then its lock released); a round waits for them.
    const cuts::Vector{Task}
    # Request files that could not be read, and how many times that has happened.
    const unread::Dict{String,Int}
end

function ControlState()
    return ControlState(
        false,
        false,
        :request,
        nothing,
        0.0,
        nothing,
        Set{String}(),
        Set{Int}(),
        Set{String}(),
        KeyFilter[],
        KeyFilter[],
        DataKey[],
        Dict{String,StopOrder}(),
        Task[],
        Dict{String,Int}(),
    )
end

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
- `ctl` — the [`ControlState`](@ref): what [`control!`](@ref) requests have changed.
- `acct` — the [`Account`](@ref): where the core-hours went, kept as it dispatches.

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
    const ctl::ControlState
    const acct::Account
    # What the last scan found among the locks (`_scan!`'s return), for the status.
    locks::Dict{String,Any}
    table::Union{TaskTable,Nothing}
    vault::Union{Vault,Nothing}
    stage::String
    state::Symbol
    multi::Bool
    interval::Float64
    last_status::Float64
    status_failed::Bool
    # Keys handed to a worker that came back because another master had taken them.
    collisions::Int
    short_since::Float64
    short_logged::Tuple{Int,Int,Int}
    # `RunOpts.stuck_after`, and the keys already reported as stuck (said once each).
    stuck_after::Float64
    stuck_said::Set{String}
    # key => when it was last seen to advance in this attempt. Kept here because the stamp it
    # comes from is removed as the key finishes, a moment before its row is settled.
    advanced::Dict{String,Float64}
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
        ControlState(),
        Account(),
        Dict{String,Any}(),
        nothing,
        nothing,
        "",
        :starting,
        false,
        0.0,
        0.0,
        false,
        0,
        0.0,
        (-1, -1, -1),
        0.0,
        Set{String}(),
        Dict{String,Float64}(),
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
            a === nothing && continue
            m.who[p] = a
            _acct_join!(m.acct, p, a.cores)
        end
    end
    return nothing
end

export state_root
