# Run — the facade that ties work_fn to Vault, Lock, Manifest, Log
#
# Evolution across todos:
#   09: minimal (sequential, no manifest, no lock)
#   10: + Manifest (early skip)
#   11: + KeyLock (multi-master)
#   12: + retry
#   13: + automatic fan-out over the Distributed workers when
#       `nprocs() > 1`, so a single master can fan out over local
#       `addprocs(n)` or a SLURM cluster allocated via
#       `SlurmClusterManager`.  No per-file change needed in user
#       compute scripts — they still call `run!(work_fn, vault, keys)`.
#   0.7: the master holds the task table (TaskTable.jl) and hands keys out itself, with the lock
#       token and the resume point; `pmap` is gone.

using Distributed
using DataVault
using ParamIO: DataKey, canonical

"""
    RunOpts(; workers=:auto, max_attempts=3, stale_after=600.0,
             heartbeat_interval=60.0, stop_flag=nothing, deadline=nothing)

Execution options for [`run!`](@ref).

# Fields

- `workers::Symbol = :auto` — dispatch mode. `:auto` fans out over the
  Distributed `workers()` when `nprocs() > 1`, and runs
  sequentially otherwise. `:sequential` forces the sequential path even when
  worker processes are present (useful for debugging a serialization issue).
- `max_attempts::Int = 3` — per-key retry budget. Set to `1` to disable
  retry (a failed `work_fn` is logged as `:error` instead of `:gave_up`).
- `stale_after::Float64 = 600.0` — seconds before another master can reclaim a
  held lock as stale. Passed through to `DataVault.acquire_running!`.
- `heartbeat_interval::Float64 = 60.0` — how often the per-lock heartbeat
  (a child process, `DataVault.start_heartbeat`) refreshes DataVault's
  `.running` file. It keeps beating while `work_fn` computes without yielding.
  Enforced to be `<` `stale_after` (otherwise a live holder's lock could be
  reclaimed mid-work).
- `log_level::Symbol = :info` — event-log verbosity. At `:info` (default) the
  high-churn per-key `:lock_busy` and `:key_start` events are suppressed (their
  totals still ride in the `:stage_done` summary), keeping the JSONL log
  O(computed keys) instead of O(masters × keys) under multi-master contention.
  Set `:debug` to log them (e.g. to debug lock contention).
- `stop_flag::Union{String,Nothing}` — path to a sentinel file. When
  `isfile(stop_flag)` becomes true, [`run!`](@ref) and [`run_loop!`](@ref)
  stop dispatching new keys and return early. This is the infra equivalent
  of FiniteTemperature.jl's `STOP_NOW_\$JOB_ID` mechanism, typically created
  by a SIGUSR1 signal handler in the batch script 60 s before Slurm kills
  the job.

  **Defaults to `ENV["SWEEPRUNNER_STOP_FLAG"]`**, because the batch script
  that traps the signal and the driver that passes the option are different
  files, and the only thing they can agree on without one importing the
  other is the environment. Leaving the name to the caller meant every
  driver had to remember a variable this package never mentions; a driver
  that misspells it gets no error and no graceful stop, only a killed job.
  Pass `stop_flag=nothing` explicitly to opt out.

  **Granularity: the flag is read between keys, not inside one.** A key already
  in `work_fn` runs to completion, so the time between raising the flag and
  `run!` returning is bounded by the longest key, which the caller usually
  cannot predict.
- `deadline::Union{Float64,Nothing} = nothing` — an absolute `time()` past which
  no new key is handed out. The same mechanism as `stop_flag` with the same
  in-key granularity, and the reason to have both is that a deadline is set in
  ADVANCE: a batch job can subtract its longest expected key and the time its
  summary needs from the end of its allocation, where a flag raised reactively
  60 s before the wall clock cannot buy back a key that runs for ten minutes.

  ```julia
  RunOpts(deadline = time() + 25 * 60)   # stop dispatching 5 min before a 30 min job ends
  ```

- `defer_poll::Float64 = 30.0` — seconds [`run!`](@ref) waits before re-dispatching keys whose
  `work_fn` threw `DataVault.ArtifactBusy` (an artifact being built by another worker or job),
  when the previous pass made no progress. A deferred key costs no attempt.
- `status_interval::Float64 = 60.0` — how often the master rewrites its status file (see
  [`read_status`](@ref)): task counts, the worker pool against what was planned, and per worker
  the key it is on, CPU utilisation and RSS. `0` writes none.

# Example

```julia
opts = RunOpts(max_attempts=5, stale_after=900.0, heartbeat_interval=30.0,
               stop_flag="/path/to/STOP_NOW_12345")
SweepRunner.run!(work_fn, vault, keys; opts)
```
"""
struct RunOpts
    workers::Symbol
    max_attempts::Int
    stale_after::Float64
    heartbeat_interval::Float64
    stop_flag::Union{String,Nothing}
    log_level::Symbol
    deadline::Union{Float64,Nothing}
    defer_poll::Float64
    status_interval::Float64
end

function RunOpts(;
    workers::Symbol=:auto,
    max_attempts::Int=3,
    stale_after::Real=600.0,
    heartbeat_interval::Real=60.0,
    stop_flag::Union{String,Nothing}=get(ENV, "SWEEPRUNNER_STOP_FLAG", nothing),
    log_level::Symbol=:info,
    deadline::Union{Real,Nothing}=nothing,
    defer_poll::Real=30.0,
    status_interval::Real=60.0,
)
    workers in (:auto, :sequential) || throw(
        ArgumentError(
            "RunOpts: workers must be :auto or :sequential, got $(repr(workers))"
        ),
    )
    log_level in (:debug, :info, :warn, :error) || throw(
        ArgumentError(
            "RunOpts: log_level must be :debug/:info/:warn/:error, got $(repr(log_level))",
        ),
    )
    heartbeat_interval < stale_after || throw(
        ArgumentError(
            "RunOpts: heartbeat_interval ($heartbeat_interval) must be < " *
            "stale_after ($stale_after), else a live holder's lock can be " *
            "reclaimed mid-work.",
        ),
    )
    return RunOpts(
        workers,
        max_attempts,
        Float64(stale_after),
        Float64(heartbeat_interval),
        stop_flag,
        log_level,
        deadline === nothing ? nothing : Float64(deadline),
        Float64(defer_poll),
        Float64(status_interval),
    )
end

# Why the loop is stopping, so `:stage_done` can say which of the two fired rather than leaving
# a reader to guess from the wall clock.
function _stop_reason(opts::RunOpts)::Union{Symbol,Nothing}
    opts.stop_flag !== nothing && isfile(opts.stop_flag) && return :flag
    opts.deadline !== nothing && time() > opts.deadline && return :deadline
    return nothing
end

# The per-key outcome vocabulary names the same two reasons `_stop_reason` does, for a
# `(key, outcome)` tuple that sits alongside `:ok` / `:error`. Written once, and loudly: a third
# reason added above must fail here rather than be silently relabelled as a deadline.
function _stop_outcome(reason::Symbol)::Symbol
    reason === :flag && return :stop_flag
    reason === :deadline && return :stop_deadline
    return throw(ArgumentError("no per-key outcome for stop reason $(repr(reason))"))
end

# How many times a key whose worker DIED is handed to another one.
const _WORKER_DEATH_REDISPATCHES = 2

# As of v0.3 the per-key lock lives ENTIRELY in DataVault's `.running`
# sentinel — acquired atomically via `DataVault.acquire_running!`
# (implemented with POSIX `link()`).  There is no longer a separate
# `locks/` directory tree maintained by this package.

"""
    manifest_root(vault) -> String

Return the directory under which [`run!`](@ref) and [`load_manifest`](@ref)
look for this vault's `manifest.jld2` — one manifest per `(project, run)`.

The layout is:

    <vault.outdir>/manifest/<project_name>/<vault.run>/manifest.jld2

Pure function; does not touch the filesystem.
"""
function manifest_root(vault::Vault)
    return joinpath(vault.outdir, "manifest", vault.spec.study.project_name)
end

"""
    load_manifest(vault::DataVault.Vault) -> Manifest

Convenience overload of the two-argument [`load_manifest`](@ref) that
derives `(root, stage)` from a `DataVault.Vault`:

    load_manifest(manifest_root(vault), Symbol(vault.run))
"""
load_manifest(vault::Vault) = load_manifest(manifest_root(vault), Symbol(vault.run))

"""
    run!(work_fn, vault, keys; opts=RunOpts(), load=nothing, affinity=nothing, observe=true,
         master=nothing) -> NamedTuple

Run `work_fn(key) -> Dict` for every `key` in `keys`, persisting through
`vault`. Writes a structured JSONL event log at
`joinpath(vault.outdir, "events_<hostname>_<pid>.jsonl")` — one file per master,
so concurrent masters never contend on a single log.

`load` names the module(s) the **worker** processes need beyond the always-loaded seam
(`ParamIO`/`DataVault`/`SweepRunner`) — typically the package or module that defines `work_fn`
and the types it touches. Accepts a `Module`, `Symbol`, `String`, or a collection of them (e.g.
`load=MyModel` or `load=[MyModel, Statistics]`). Under `:distributed`/`:slurm`, `run!` `using`s
these in `Main` on every worker before fan-out, so a compute script no longer has to hand-roll the
`for w in workers(); remotecall_fetch(…, :(using …)); end` broadcast. It is a no-op on the master
(`nprocs() == 1`) and idempotent, so it is safe even when a project still broadcasts by hand.

Early skip (todo 10): on startup a stage-level Manifest is loaded. Keys
already in the manifest are skipped — when all keys are done, the second
run-through takes O(1) filesystem operations regardless of `length(keys)`.

# Source observations

With `observe=true` (the default) the master and every worker call `DataVault.observe_sources`
before any key is dispatched, and each `.done` a process writes carries that process's token
(`observation=<token>`). The observation records what the source looked like at `run!` start and
its **binding** — how far the code that process had loaded was checked against it — so a marker
never claims more than was checked. An observation that fails does not stop the run: the event log
says why, and that process's markers read `observation=unknown`, as they do with `observe=false`.

# Affinity

`affinity` is `key -> value`, and turns the fan-out from "any free worker takes the next key" into
"a free worker PREFERS a key whose `affinity` value it has already handled". Pass it when `work_fn`
memoises something per group in worker-local state, so a worker that stays on a group pays the load
once instead of once per key.

    run!(work_fn, vault, keys; affinity = k -> param(k, "system.L"))

A preference, not a partition: a worker is never idle while a key is pending, so a 200-key group
does not serialise onto the worker that opened it. When a worker has nothing from its own groups
left it takes from the group with the most work outstanding, which spreads workers over groups.

Only affects the fan-out; the sequential path visits keys in order.

# The task table

The master reads the markers once, before it dispatches: keys finished by a sibling since the
manifest was written are settled, a lock whose holder is provably gone is removed, and a key held
by a live sibling is not handed out at all (it is counted in `busy`). What is left is the queue.
Each key goes to a worker together with its lock token and the last progress recorded for it, so
`work_fn` can ask [`resume_point`](@ref) instead of probing its own outputs; see
[`report_progress`](@ref).

When the queue drains, the keys that came back busy are asked about once more, since their holder
may have finished or died while the pass ran.

`master` is the [`Master`](@ref) this call runs as (a fresh one by default). A caller that makes
several `run!` calls as one job (as [`run_loop!`](@ref) does) passes the same one to each, so they
share an event log, a status file and the worker identities already collected.

# Status

While it runs, the master rewrites `<state_root>/masters/<id>/status.json` every
`opts.status_interval` seconds; [`read_status`](@ref) / [`print_status`](@ref) read it from any
process, during the job or after it.

Returns `(; stage, done, err, busy, gave_up, stop, skipped, total, stopped_by)`. `stopped_by` is
`:flag`, `:deadline`, or `nothing`: a stage that finished every key reports `nothing` even if the
deadline passed while its last key ran, since no key was ever held back by it.
The full-done early exit returns the same field set rather than a shorter one.

Contract:
- `work_fn` is expected to be a pure function: given a `DataKey`, return a
  `Dict` payload to persist via `DataVault.save!`.
- Exceptions in `work_fn` are caught and logged; the corresponding key's
  `.done` file is not written, so re-runs will pick it up.
- The stage label used for logging is `Symbol(vault.run)`.
- Manifest is monotonic: saved at end-of-stage with every newly completed key.

# Parallel dispatch

If `nprocs() > 1` (i.e. `init_workers!(mode=:distributed|:slurm)` has added
worker processes), `run!` automatically fans out over the Distributed
`workers()`.  Each worker runs the per-key
lock-acquire → `work_fn` → `DataVault.save!` → `mark_done!` pipeline
for the key it was handed.  All filesystem operations (the `.running` lock, atomic JLD2 write,
JSONL event log) are already NFS-safe, so concurrent workers inside one
master are structurally consistent with multi-master operation.

If only the master is active (`nprocs() == 1`), `run!` falls back to the
sequential loop from todo 11.  This means the same compute.jl script is
valid in three modes:

1. No `init_workers!` call at all → sequential on the master.
2. `init_workers!(mode=:distributed)` with `addprocs(n)` → local fan-out.
3. `init_workers!(mode=:slurm)` inside a SLURM job → cluster fan-out.

Multi-master locking (several separate julia processes writing to the
same vault) continues to work underneath either path because the lock
layer (DataVault's `.running`) uses POSIX `link()` / atomic `rename` only.
"""
function run!(
    work_fn::Function,
    vault::Vault,
    keys::AbstractVector{DataKey};
    opts::RunOpts=RunOpts(),
    load=nothing,
    affinity=nothing,
    observe::Bool=true,
    master::Union{Master,Nothing}=nothing,
)
    stage = Symbol(vault.run)
    # A master handed in outlives this call (`run_loop!` between rounds); one made here does not.
    own = master === nothing
    master = own ? Master() : master
    log_name = "events_$(master.id).jsonl"
    log = EventLog(joinpath(vault.outdir, log_name); min_level=opts.log_level)
    multi = opts.workers !== :sequential && nprocs() > 1
    master.vault = vault
    master.stage = String(stage)
    master.multi = multi
    master.interval = opts.status_interval
    after = own ? :ended : :waiting

    # Early skip: load manifest, subtract completed keys
    m = load_manifest(vault)
    todo = todo_keys(m, collect(keys))

    if isempty(todo)
        log_event(log, :skip_complete; stage=stage, total=length(keys))
        master.table = nothing
        master.state = after
        master.interval > 0 && write_status(master)
        return (
            stage=stage,
            done=0,
            err=0,
            busy=0,
            gave_up=0,
            stop=0,
            skipped=length(keys),
            total=length(keys),
            stopped_by=nothing,
        )
    end

    log_event(log, :stage_start; stage=stage, total=length(keys), todo=length(todo))

    # Dispatch strategy: fan out when Distributed workers are present (unless the
    # caller forced `workers=:sequential`), otherwise draw the queue on this process.
    if multi
        # Ensure the seam packages (+ the user's work module(s) via `load=`) are loaded in `Main`
        # on every worker before fan-out. `init_workers!` spawns workers with `--project` but loads
        # no packages, so the first dispatched key would otherwise die with a cryptic
        # `KeyError: <Module> not found` (DataKey deserialization / the save! pipeline / work_fn).
        # Idempotent, so it composes with a project that still broadcasts modules by hand.
        _ensure_worker_modules(
            vcat([:ParamIO, :DataVault, :SweepRunner], _worker_module_names(load))
        )
    end
    # Every process that will write markers observes its sources now, so each `.done` names the
    # observation of the process that computed it (see Observe.jl).
    _observe_processes!(vault, multi, observe, log, stage)
    # The master's view of the round: one pass over the markers, then the queue the dispatcher
    # draws from. The sequential path visits keys in the caller's order, so it takes no affinity.
    table = TaskTable(todo; affinity=multi ? affinity : nothing)
    _scan!(table, vault, stage, log, opts)
    master.table = table
    master.state = :running
    _identify_workers!(master, multi ? workers() : [myid()])
    drive = if multi
        () -> _drive_workers!(work_fn, vault, table, stage, log, opts, master)
    else
        () -> _drive_sequential!(work_fn, vault, table, stage, log, opts, master)
    end
    try
        _with_status(master, log) do
            return _dispatch!(drive, table, vault, stage, log, opts)
        end
    finally
        # Also on the way out through an exception: a status that still says `running` after the
        # master has left is the one thing it must not say.
        master.state = after
        master.interval > 0 && status_tick!(master, log)
    end

    # Aggregate outcomes into counters + manifest updates.
    n_done = 0
    n_err = 0
    n_busy = 0
    n_gave_up = 0
    n_stop = 0
    stop_seen = nothing
    for row in table.rows
        key, outcome = row.key, row.outcome
        if outcome === :already_done
            add_complete!(m, key)
        elseif outcome === :ok
            add_complete!(m, key)
            n_done += 1
        elseif outcome === :stop_flag
            n_stop += 1
            stop_seen = :flag                 # outranks :deadline, as `_stop_reason` does
        elseif outcome === :stop_deadline
            n_stop += 1
            stop_seen === nothing && (stop_seen = :deadline)
        elseif outcome === :gave_up
            n_gave_up += 1
            n_err += 1
        elseif outcome === :error
            n_err += 1
        else
            # `:lock_busy`, `:deferred`, `:worker_lost`: never attempted to completion here, and
            # retriable.
            n_busy += 1
        end
    end

    # Persist the updated manifest, merging with on-disk state so that
    # concurrent masters don't overwrite each other's completed keys.
    merge_and_save_manifest!(m)

    # From what the round actually did, so a stage that finished every key is not attributed to a
    # deadline that passed while the last one ran.
    stopped_by = stop_seen
    log_event(
        log,
        :stage_done;
        stage=stage,
        total=length(keys),
        done=n_done,
        err=n_err,
        busy=n_busy,
        gave_up=n_gave_up,
        stop=n_stop,
        skipped=length(keys) - length(todo),
        stopped_by=stopped_by === nothing ? nothing : String(stopped_by),
    )
    return (
        stage=stage,
        done=n_done,
        err=n_err,
        busy=n_busy,
        gave_up=n_gave_up,
        stop=n_stop,
        skipped=length(keys) - length(todo),
        total=length(keys),
        stopped_by=stopped_by,
    )
end

# Clear a `.running` whose holder is provably gone, so the key is retriable NOW rather than in
# `stale_after`. Returns whether anything was cleared.
#
# Only `:dead` acts. `:unknown` is the common answer (a holder on another host with no Slurm id)
# and leaves the timeout to decide, exactly as before.
function _reap_if_dead!(vault::Vault, key::DataKey, stage::Symbol, log::EventLog)::Bool
    # An `isfile` first: `running_owner` opens and reads, and the uncontended case is every key.
    DataVault.is_running(vault, key) || return false
    # Reaping is an OPTIMISATION over `stale_after`, so nothing in it may be fatal. Without this,
    # an unlink that fails (a read-only status directory, an NFS hiccup) escapes `run!` and takes
    # every other key in the round with it, none of which was attempted.
    try
        owner = DataVault.running_owner(vault, key)
        owner === nothing && return false      # unstamped: cannot be attributed, so cannot be judged
        holder_liveness(owner) === :dead || return false
        cleared = DataVault.clear_running!(vault, key, owner)
        cleared &&
            log_event(log, :lock_reaped; stage=stage, key=canonical(key), owner=owner)
        return cleared
    catch e
        e isa InterruptException && rethrow()
        log_event(log, :reap_failed; stage=stage, key=canonical(key), err=_short_err(e))
        return false
    end
end

"""
    _run_one_with_lock!(work_fn, vault, key, stage, log, opts) -> (DataKey, Symbol)

Execute the per-key pipeline: atomic-acquire via
`DataVault.acquire_running!`, re-check completion, run work_fn with a
heartbeat child process, commit owner-checked, and release on exit.  Returns a
`(key, outcome)` pair suitable for aggregation by the caller.

Outcome symbols:
- `:lock_busy`    — another master holds a fresh `.running`, skipped.
- `:already_done` — finished by a sibling master between the manifest
                    read and the lock acquisition.
- `:ok`           — `work_fn` succeeded and `mark_done!` was called.
- `:error`        — single-attempt failure (`opts.max_attempts == 1`).
- `:gave_up`      — all `opts.max_attempts` attempts failed.
- `:stop_flag` / `:stop_deadline`
                  a stop condition held before work started, carrying which one.
"""
function _run_one_with_lock!(
    work_fn::Function,
    vault::Vault,
    key::DataKey,
    stage::Symbol,
    log::EventLog,
    opts::RunOpts;
    tok::AbstractString=owner_token(),
    resume::Union{Progress,Nothing}=nothing,
    reap::Bool=true,
)
    kstr = canonical(key)

    # Early exit if stop flag has been raised (checked here as well as by the dispatcher, so a
    # key already on its way to a worker is not started).
    # The reason travels back WITH the outcome: a flag file can be removed and a deadline can pass
    # before the outcome is read, so re-deriving it later can name something that did not stop this.
    stop = _stop_reason(opts)
    stop === nothing || return (key, _stop_outcome(stop))

    # A lock whose holder can be SHOWN to be gone does not have to wait out `stale_after`. The
    # clear is owner-checked, so it is a no-op if the holder changed since the question was asked.
    # Under `run!` the master asked this for every key before dispatching (`_scan!`), and `reap`
    # is false.
    reap && _reap_if_dead!(vault, key, stage, log)

    # DataVault owns the lock file.  `acquire_running!` is atomic on
    # NFS via POSIX `link()`: concurrent masters see at most one
    # `:ok` / `:reclaimed`; the losers see `:busy`. `tok` names this acquisition; under `run!` the
    # master made it, so its table says who holds what.
    acq = DataVault.acquire_running!(vault, key, tok; stale_after=opts.stale_after)
    if acq === :busy
        log_event(log, :lock_busy; level=:debug, stage=stage, key=kstr)
        return (key, :lock_busy)
    end
    # acq ∈ (:ok, :reclaimed) — we own the lock.

    # Written at ACQUIRE, at :info, and flushed by `log_event`'s open/write/close. This is the
    # only record that survives a SIGKILL mid-key: the `finally` below cannot run, so nothing
    # later in this function gets to say the key was ever claimed.
    log_event(log, :key_acquired; stage=stage, key=kstr, acq=String(acq))

    # Re-check after acquisition: another master may have finished this
    # key between our manifest read and our acquire.
    if DataVault.is_done(vault, key)
        DataVault.clear_running!(vault, key, tok)
        return (key, :already_done)
    end

    # The heartbeat runs in a CHILD process (DataVault 0.8.9). A task here was starved by work that
    # does not yield — a long BLAS call, a tight loop — under -t 1, -t 2 and -t 2,1 alike: it never
    # beat, a sibling reclaimed the live key after `stale_after`, and this master committed it too.
    # The child stops when this process dies or when the lock leaves our hands.
    #
    # Whether we still hold the key is asked where it matters, at commit: `_run_one_with_retry!`
    # checks the owner before `save!`, and commits with the owner form of `mark_done!`, which
    # refuses if a sibling reclaimed in between. The release below is owner-checked as well, so it
    # can run unconditionally: it never deletes a reclaimer's lock.
    hb = DataVault.start_heartbeat(vault, key, tok; interval=opts.heartbeat_interval)

    outcome = try
        _run_one_with_retry!(work_fn, vault, key, kstr, stage, log, opts, tok; resume=resume)
    finally
        DataVault.stop_heartbeat(hb)
        # Release so a sibling can retry the key at once instead of after `stale_after`. On `:ok`
        # the commit already released it; on a lost key the lock is the reclaimer's, and this is
        # a no-op.
        DataVault.clear_running!(vault, key, tok)
    end

    return (key, outcome)
end

# What `_scan_row!` found for one key.
#   :done   — finished already (by a sibling, since the manifest was written)
#   :free   — no lock; queue it
#   :reaped — a lock whose holder is provably gone was removed; queue it
#   :stale  — a lock past `stale_after`; queue it, `acquire_running!` reclaims
#   :held   — a live or not-yet-stale lock; do not queue it this pass
function _scan_row!(
    table::TaskTable,
    i::Int,
    vault::Vault,
    stage::Symbol,
    log::EventLog,
    opts::RunOpts,
    progress::Dict{String,Progress},
)::Symbol
    r = table.rows[i]
    if DataVault.is_done(vault, r.key)
        settle!(table, i, :already_done)
        return :done
    end
    r.progress = get(progress, r.kstr, nothing)
    DataVault.is_running(vault, r.key) || return :free
    _reap_if_dead!(vault, r.key, stage, log) && return :reaped
    DataVault.running_age_secs(vault, r.key) > opts.stale_after && return :stale
    hold!(table, i, DataVault.running_owner(vault, r.key))
    log_event(log, :lock_busy; level=:debug, stage=stage, key=r.kstr)
    return :held
end

"""
    _scan!(table, vault, stage, log, opts) -> NamedTuple

The master's one pass over the markers: every queued row is checked for a `.done` written since
the manifest and for a `.running` lock, and the recorded progress is attached. Returns
`(; done, held, reaped, stale)`.

This is the read the workers used to do one key at a time. A key that turns out to be locked by a
live sibling is not dispatched at all.
"""
function _scan!(table::TaskTable, vault::Vault, stage::Symbol, log::EventLog, opts::RunOpts)
    progress = read_progress(vault)
    done = held = reaped = stale = 0
    for i in eachindex(table.rows)
        table.rows[i].state === :todo || continue
        s = _scan_row!(table, i, vault, stage, log, opts, progress)
        s === :done && (done += 1)
        s === :held && (held += 1)
        s === :reaped && (reaped += 1)
        s === :stale && (stale += 1)
    end
    return (; done, held, reaped, stale)
end

# How many times a drained queue looks again at the keys it could not get. Their holder may have
# finished, died or released while this pass ran, and a long pass is hours.
const _BUSY_RESCANS = 2

# Ask again about every key that came back `:lock_busy`, and requeue the ones that are free now.
# Returns how many were requeued.
function _rescan_busy!(
    table::TaskTable, vault::Vault, stage::Symbol, log::EventLog, opts::RunOpts
)::Int
    busy = [i for (i, r) in enumerate(table.rows) if r.outcome === :lock_busy]
    isempty(busy) && return 0
    progress = read_progress(vault)
    n = 0
    for i in busy
        s = _scan_row!(table, i, vault, stage, log, opts, progress)
        (s === :free || s === :reaped || s === :stale) || continue
        requeue!(table, i)
        n += 1
    end
    return n
end

function _n_finished(table::TaskTable)
    return count(r -> r.outcome === :ok || r.outcome === :already_done, table.rows)
end

"""
    _dispatch!(drive, table, vault, stage, log, opts)

Run `drive()` (one pass: the queue is drawn until it is empty and nothing is running) until the
table has nothing left that another pass could finish.

Two things put a row back on the queue between passes. A key that came back `:lock_busy` is asked
about again, since its holder may be gone by now. A key whose `work_fn` threw
`DataVault.ArtifactBusy` is re-dispatched: at once after a pass that finished something (the
artifact it waited on has usually been built by then), after `opts.defer_poll` seconds otherwise
(the builder is then another job). A key still deferred when the run stops stays `:deferred`,
which `run!` counts with `busy`: it was never attempted.
"""
function _dispatch!(
    drive, table::TaskTable, vault::Vault, stage::Symbol, log::EventLog, opts::RunOpts
)
    round = 0
    while true
        before = _n_finished(table)
        for _ in 0:_BUSY_RESCANS
            drive()
            _stop_reason(opts) === nothing || break
            _rescan_busy!(table, vault, stage, log, opts) == 0 && break
        end
        deferred = [i for (i, r) in enumerate(table.rows) if r.outcome === :deferred]
        isempty(deferred) && break
        _stop_reason(opts) === nothing || break
        _n_finished(table) > before || sleep(opts.defer_poll)
        round += 1
        log_event(log, :deferred_round; stage=stage, round=round, keys=length(deferred))
        foreach(i -> requeue!(table, i), deferred)
    end
    return nothing
end

"""
    _drive_sequential!(work_fn, vault, table, stage, log, opts[, master])

Draw the queue on this process, one key at a time.
"""
function _drive_sequential!(
    work_fn::Function,
    vault::Vault,
    table::TaskTable,
    stage::Symbol,
    log::EventLog,
    opts::RunOpts,
    master::Union{Master,Nothing}=nothing,
)
    while true
        # No timer fires while this process is inside `work_fn`, so the status is refreshed here,
        # between keys.
        master === nothing || _status_due!(master, log)
        # The keys a stop drops are ATTRIBUTED, not silently absent: every row ends the round
        # with an outcome.
        stop = _stop_reason(opts)
        if stop !== nothing
            settle_queued!(table, _stop_outcome(stop))
            break
        end
        i = next_task!(table, myid())
        i === nothing && break
        row = table.rows[i]
        tok = owner_token()
        start_task!(table, i, tok, myid())
        (_, outcome) = _run_one_with_lock!(
            work_fn,
            vault,
            row.key,
            stage,
            log,
            opts;
            tok=tok,
            resume=row.progress,
            reap=false,
        )
        settle!(table, i, outcome)
    end
    return nothing
end

# A worker exited holding `row`'s lock. The master named that lock, so it can take it back now
# instead of leaving it for `stale_after`: the heartbeat died with the worker, and whoever is
# handed the key next would otherwise find it busy. Owner-checked, so it removes nothing a sibling
# has since reclaimed; and if the worker is in fact alive and only unreachable, its commit is
# owner-checked too and is refused.
function _release_dead!(
    vault::Vault, row::TaskRow, tok::AbstractString, stage::Symbol, log::EventLog
)
    try
        DataVault.clear_running!(vault, row.key, tok) && log_event(
            log,
            :lock_released;
            stage=stage,
            key=row.kstr,
            owner=tok,
            why="worker_exited",
        )
    catch e
        e isa InterruptException && rethrow()
        log_event(log, :reap_failed; stage=stage, key=row.kstr, err=_short_err(e))
    end
    return nothing
end

"""
    _drive_workers!(work_fn, vault, table, stage, log, opts, master)

Draw the queue over the Distributed workers: one dispatch task per worker, each taking the next
row [`next_task!`](@ref) gives it, handing the worker the key WITH its lock token and resume
point, and settling the row with what comes back.

The master names the lock (`owner_token(host, pid)` of the worker), so the table knows who holds
what while it runs, and a worker that dies has its lock released at once.

A dispatch task does not leave while a key is still out: a worker that dies gives its key back,
and somebody has to be there to take it. A key that has taken down
`_WORKER_DEATH_REDISPATCHES + 1` workers is reported rather than handed to the next one —
unbounded, a key that reliably kills whoever takes it is handed to worker after worker forever.
"""
function _drive_workers!(
    work_fn::Function,
    vault::Vault,
    table::TaskTable,
    stage::Symbol,
    log::EventLog,
    opts::RunOpts,
    master::Master,
)
    pids = workers()
    _identify_workers!(master, pids)
    who = lock(() -> copy(master.who), master.lock)
    # All dispatch tasks are `@async` on this task's thread, so a plain counter and Condition are
    # enough: nothing between a check and the `wait` that follows it can yield.
    out = Ref(0)
    idle = Condition()

    stopped = Ref(false)

    function _loop(pid::Int, host::String, ospid::Int)
        while true
            if !stopped[]
                stop = _stop_reason(opts)
                if stop !== nothing
                    settle_queued!(table, _stop_outcome(stop))
                    stopped[] = true
                end
            end
            # A worker that went away while this loop was waiting must not be handed a key: the
            # call would fail at once and be counted against the key as a death.
            pid in workers() || break
            i = next_task!(table, pid)
            if i === nothing
                out[] == 0 && break
                wait(idle)
                continue
            end
            row = table.rows[i]
            tok = owner_token(host, ospid)
            start_task!(table, i, tok, pid)
            out[] += 1
            died = false
            outcome = try
                last(
                    remotecall_fetch(
                        _run_one_with_lock!,
                        pid,
                        work_fn,
                        vault,
                        row.key,
                        stage,
                        log,
                        opts;
                        tok=tok,
                        resume=row.progress,
                        reap=false,
                    ),
                )
            catch e
                if e isa ProcessExitedException
                    died = true
                    _release_dead!(vault, row, tok, stage, log)
                    row.deaths += 1
                    if row.deaths > _WORKER_DEATH_REDISPATCHES
                        log_event(
                            log,
                            :gave_up;
                            stage=stage,
                            key=row.kstr,
                            attempts=row.deaths,
                            err="worker exited on this key every time it was dispatched",
                        )
                        :error
                    else
                        nothing            # goes back on the queue
                    end
                else
                    log_event(
                        log,
                        :error;
                        stage=stage,
                        key=row.kstr,
                        attempt=0,
                        err=_short_err(e),
                    )
                    :error
                end
            finally
                out[] -= 1
            end
            if outcome === nothing
                # What it had reported before it died is where the next worker starts.
                row.progress = _read_progress_one(vault, row.kstr)
                requeue!(table, i; front=true)
            else
                settle!(table, i, outcome)
            end
            notify(idle)
            died && break
        end
        return nothing
    end

    @sync for pid in pids
        w = get(who, pid, nothing)
        w === nothing && continue
        @async try
            _loop(pid, w.host, w.pid)
        finally
            # A loop that leaves on an exception must not strand the ones waiting on it.
            notify(idle)
        end
    end

    # Every worker died while keys were still pending. Those keys were never attempted, so they are
    # retriable rather than failed, and `run!` counts them with `busy`.
    for (i, r) in enumerate(table.rows)
        r.state === :todo || continue
        log_event(log, :worker_lost; stage=stage, key=r.kstr)
        settle!(table, i, :worker_lost)
    end
    return nothing
end

"""
    _run_one_with_retry!(work_fn, vault, key, kstr, stage, log, opts, tok) -> Symbol

Execute `work_fn(key)` up to `opts.max_attempts` times. Returns:
  :ok        — payload saved and mark_done! called
  :gave_up   — all attempts failed, final `:gave_up` event logged
  :error     — single-attempt config (`max_attempts == 1`) that failed once
  :lock_busy — the lock is no longer `tok`'s (a sibling reclaimed it): the result is
               discarded, before `save!` if the loss is already visible, else at the
               owner-checked commit, so the reclaiming master's result wins
"""
function _run_one_with_retry!(
    work_fn,
    vault::Vault,
    key::DataKey,
    kstr::String,
    stage::Symbol,
    log::EventLog,
    opts::RunOpts,
    tok::AbstractString;
    resume::Union{Progress,Nothing}=nothing,
)
    last_err = nothing
    reported = Ref(resume !== nothing)
    for attempt in 1:opts.max_attempts
        log_event(log, :key_start; level=:debug, stage=stage, key=kstr, attempt=attempt)
        t0 = time()
        try
            # What `work_fn` can ask about the key it was handed (`resume_point`,
            # `report_progress`). A retry starts from what the failed attempt reported.
            if attempt > 1 && reported[]
                resume = _read_progress_one(vault, kstr)
            end
            ctx = KeyContext(vault, key, kstr, String(tok), resume, opts, reported)
            payload = with(() -> work_fn(key), _KEY => ctx)
            payload isa Dict || error(
                "work_fn must return a Dict (got $(typeof(payload))). " *
                "Wrap scalars as e.g. Dict(\"value\" => x).",
            )
            if DataVault.running_owner(vault, key) != tok
                # A sibling master reclaimed our lock while work_fn ran; it now
                # owns this key. Discard our result rather than double-committing.
                log_event(log, :lock_lost; stage=stage, key=kstr, attempt=attempt)
                return :lock_busy
            end
            # The digest save! took before its rename goes into the marker, so `.done` names the
            # bytes this attempt wrote rather than whatever the file holds when someone looks.
            saved = DataVault.save!(vault, key, payload)
            # Owner-checked: a reclaim between the check above and here is refused, and nothing is
            # committed. The file save! wrote is then the reclaimer's to overwrite.
            committed = DataVault.mark_done!(
                vault, key, tok; result=saved, observation=_observation_token(vault)
            )
            if !committed
                log_event(log, :lock_lost; stage=stage, key=kstr, attempt=attempt)
                return :lock_busy
            end
            # A finished unit has no resume point. Only when one was written, so a unit that
            # never reports costs no extra filesystem call.
            reported[] && _clear_progress(vault, kstr)
            log_event(
                log,
                :key_done;
                stage=stage,
                key=kstr,
                secs=time() - t0,
                attempt=attempt,
                sha256=saved.sha256,
            )
            return :ok
        catch e
            # Not a failure: the artifact this key needs is being built elsewhere. Hand the key
            # back without spending an attempt; `run!` re-dispatches it once the pass drains.
            if e isa DataVault.ArtifactBusy
                log_event(log, :artifact_busy; stage=stage, key=kstr, artifact=e.name)
                return :deferred
            end
            last_err = _short_err(e)
            log_event(log, :error; stage=stage, key=kstr, attempt=attempt, err=last_err)
            if attempt < opts.max_attempts
                log_event(log, :retry; stage=stage, key=kstr, next_attempt=attempt + 1)
                sleep(0.1 * attempt)  # linear backoff
            end
        end
    end
    log_event(
        log, :gave_up; stage=stage, key=kstr, attempts=opts.max_attempts, err=last_err
    )
    return opts.max_attempts == 1 ? :error : :gave_up
end

# Truncate a (potentially huge, e.g. full-stacktrace) error string so a single
# JSONL event line stays under PIPE_BUF, preserving the O_APPEND cross-process
# atomicity of the event log.
function _short_err(e)::String
    s = sprint(showerror, e)
    return length(s) > 2000 ? string(first(s, 2000), " …[truncated]") : s
end

"""
    run_loop!(work_fn, vault, keys; opts=RunOpts(), max_empty_rounds=3,
              idle_sleep=30.0, load=nothing, prerequisite=nothing) -> NamedTuple

Work-stealing loop that repeatedly calls [`run!`](@ref) until there is no
more work to do. This is the infra equivalent of FiniteTemperature.jl's
`_work_loop` driver.

The loop exits when:
- `max_empty_rounds` consecutive rounds produce zero new completions AND leave nothing held by a
  sibling, or
- `opts.stop_flag` is raised, or `opts.deadline` has passed.

A round that completes nothing but finds keys `:lock_busy` does NOT count toward
`max_empty_rounds` until `opts.stale_after` has been waited out. Those keys are either being
worked on by a live sibling, or held by one the wall clock killed, and `stale_after` is what
separates the two: past it, `acquire_running!` reclaims the lock on the next attempt. Returning
before then leaves the campaign short and reports nothing, because `max_empty_rounds *
idle_sleep` (90 s by default) is an order of magnitude under `stale_after` (600 s).

Default parameters (`max_empty_rounds=3`, `idle_sleep=30.0`) are the
battle-tested values from FiniteTemperature.jl.

`load` is forwarded verbatim to every [`run!`](@ref) call (see its docstring) — name the work
module(s) the workers need and the loop handles the per-round broadcast.

# Prerequisite

`run!` locks the KEY, so no two workers compute the same key. Work shared BETWEEN keys has to live
inside `work_fn`, and there it has no protection at all: every worker that wants a setup not yet on
disk builds it itself.

Pass a [`Prerequisite`](@ref) and that setup becomes its own key space, run to completion by
[`run_prerequisite!`](@ref) before the dependent stage starts. It then gets the same locking,
resume and provenance as any other stage, and its cost is recorded in its own payload instead of
landing on whichever dependent key happened to run first.

    run_loop!(work_fn, vault, keys;
              prerequisite = Prerequisite(prep_fn, prep_vault, derived_keys),
              opts = opts)

If the prerequisite does not complete, the dependent stage does NOT start, and the returned
`prerequisite` field says why. Running it anyway would spend the allocation on keys whose setup is
known to be missing.

**SweepRunner does not know which dependent key needs which prerequisite key.** The dependency is
one level deep and resolved inside `work_fn`, so this is "all of the prerequisite, then all of the
dependents", not a DAG.

`affinity` is forwarded verbatim to every [`run!`](@ref) call.

Returns `(; ran, rounds, done, busy, stopped_by, prerequisite)`. `busy` is how many keys the last
round found held by a sibling, so a caller can tell "everything is done" from "someone else still
has work out". `ran` is `false` exactly when a prerequisite blocked the stage.
"""
function run_loop!(
    work_fn::Function,
    vault::Vault,
    keys::AbstractVector{DataKey};
    opts::RunOpts=RunOpts(),
    max_empty_rounds::Int=3,
    idle_sleep::Float64=30.0,
    load=nothing,
    prerequisite=nothing,
    affinity=nothing,
    observe::Bool=true,
)
    pre = nothing
    if prerequisite !== nothing
        pre = run_prerequisite!(prerequisite; opts=opts, load=load, poll=idle_sleep)
        pre.complete || return (;
            ran=false,
            rounds=0,
            done=0,
            busy=0,
            stopped_by=pre.stopped_by,
            prerequisite=pre,
        )
    end

    # One master for every round: its event log, and what it has learned about its workers.
    master = Master()
    empty_count = 0
    rounds = 0
    n_done = 0
    n_busy = 0
    busy_waited = 0.0
    # A lock is reclaimable once its heartbeat is `stale_after` old, so waiting that long is what
    # separates "a sibling is working on it" from "the holder is gone". The margin covers the round
    # that has to follow the expiry to act on it.
    busy_budget = opts.stale_after + 2 * idle_sleep
    stopped = nothing
    while true
        # Captured at the exit rather than re-read at return. A loop that exhausts
        # `max_empty_rounds` sleeps `idle_sleep` between rounds and can cross the deadline while
        # doing so, and a flag file removed in the meantime turns a real flag stop into `nothing`.
        stopped = _stop_reason(opts)
        stopped === nothing || break
        rounds += 1
        result = run!(
            work_fn,
            vault,
            keys;
            opts=opts,
            load=load,
            affinity=affinity,
            observe=observe,
            master=master,
        )
        n_done += result.done
        n_busy = result.busy
        if result.done > 0
            empty_count = 0
            busy_waited = 0.0
            continue
        end
        # A round that completed nothing but found keys held by a SIBLING is not an empty round:
        # either that sibling finishes them, or it is dead and `acquire_running!` reclaims them
        # once its heartbeat passes `stale_after`. Counting it as empty is what made a follow-on
        # job return after `max_empty_rounds * idle_sleep` while the locks stayed held for
        # `stale_after`, leaving the campaign short and saying nothing.
        if result.busy > 0 && busy_waited < busy_budget
            busy_waited += idle_sleep
            sleep(idle_sleep)
            continue
        end
        empty_count += 1
        if empty_count >= max_empty_rounds
            # The round itself may have been cut short rather than empty, and if so that is why
            # there was nothing to do. Its own recorded reason, not a fresh clock read.
            stopped = result.stopped_by
            break
        end
        sleep(idle_sleep)
    end
    master.state = :ended
    master.interval > 0 && write_status(master)
    return (;
        ran=true,
        rounds=rounds,
        done=n_done,
        busy=n_busy,
        stopped_by=stopped,
        prerequisite=pre,
    )
end

export RunOpts, run!, run_loop!, manifest_root, load_manifest
