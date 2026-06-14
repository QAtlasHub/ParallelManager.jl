# Run — the facade that ties work_fn to Vault, Lock, Manifest, Log
#
# Evolution across todos:
#   09: minimal (sequential, no manifest, no lock)
#   10: + Manifest (early skip)
#   11: + KeyLock (multi-master)
#   12: + retry
#   13: + automatic `pmap(WorkerPool(workers()), ...)` dispatch when
#       `nprocs() > 1`, so a single master can fan out over local
#       `addprocs(n)` or a SLURM cluster allocated via
#       `SlurmClusterManager`.  No per-file change needed in user
#       compute scripts — they still call `run!(work_fn, vault, keys)`.

using Distributed
using DataVault
using ParamIO: DataKey, canonical

"""
    RunOpts(; workers=:auto, max_attempts=3, stale_after=600.0,
             heartbeat_interval=60.0, stop_flag=nothing)

Execution options for [`run!`](@ref).

# Fields

- `workers::Symbol = :auto` — dispatch mode. `:auto` fans out over the
  Distributed `WorkerPool(workers())` via `pmap` when `nprocs() > 1`, and runs
  sequentially otherwise. `:sequential` forces the sequential path even when
  worker processes are present (useful for debugging a serialization issue).
- `max_attempts::Int = 3` — per-key retry budget. Set to `1` to disable
  retry (a failed `work_fn` is logged as `:error` instead of `:gave_up`).
- `stale_after::Float64 = 600.0` — seconds before another master can reclaim a
  held lock as stale. Passed through to `DataVault.acquire_running!`.
- `heartbeat_interval::Float64 = 60.0` — how often the per-lock heartbeat task
  refreshes DataVault's `.running` file. Enforced to be `<` `stale_after`
  (otherwise a live holder's lock could be reclaimed mid-work).
- `log_level::Symbol = :info` — event-log verbosity. At `:info` (default) the
  high-churn per-key `:lock_busy` and `:key_start` events are suppressed (their
  totals still ride in the `:stage_done` summary), keeping the JSONL log
  O(computed keys) instead of O(masters × keys) under multi-master contention.
  Set `:debug` to log them (e.g. to debug lock contention).
- `stop_flag::Union{String,Nothing} = nothing` — path to a sentinel file.
  When `isfile(stop_flag)` becomes true, [`run!`](@ref) and
  [`run_loop!`](@ref) stop dispatching new keys and return early. This is
  the infra equivalent of FiniteTemperature.jl's `STOP_NOW_\$JOB_ID`
  mechanism, typically created by a SIGUSR1 signal handler in the batch
  script 60 s before Slurm kills the job.

# Example

```julia
opts = RunOpts(max_attempts=5, stale_after=900.0, heartbeat_interval=30.0,
               stop_flag="/path/to/STOP_NOW_12345")
ParallelManager.run!(work_fn, vault, keys; opts)
```
"""
struct RunOpts
    workers::Symbol
    max_attempts::Int
    stale_after::Float64
    heartbeat_interval::Float64
    stop_flag::Union{String,Nothing}
    log_level::Symbol
end

function RunOpts(;
    workers::Symbol=:auto,
    max_attempts::Int=3,
    stale_after::Real=600.0,
    heartbeat_interval::Real=60.0,
    stop_flag::Union{String,Nothing}=nothing,
    log_level::Symbol=:info,
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
    )
end

# Internal: check if the stop flag has been raised.
_is_stopped(opts::RunOpts)::Bool = opts.stop_flag !== nothing && isfile(opts.stop_flag)

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
    run!(work_fn, vault, keys; opts=RunOpts()) -> NamedTuple

Run `work_fn(key) -> Dict` for every `key` in `keys`, persisting through
`vault`. Writes a structured JSONL event log at
`joinpath(vault.outdir, "events.jsonl")`.

Early skip (todo 10): on startup a stage-level Manifest is loaded. Keys
already in the manifest are skipped — when all keys are done, the second
run-through takes O(1) filesystem operations regardless of `length(keys)`.

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
`WorkerPool(workers())` via `pmap`.  Each worker runs the per-key
lock-acquire → `work_fn` → `DataVault.save!` → `mark_done!` pipeline
independently.  All filesystem operations (the `.running` lock, atomic JLD2 write,
JSONL event log) are already NFS-safe, so concurrent workers inside one
master are structurally consistent with multi-master operation.

If only the master is active (`nprocs() == 1`), `run!` falls back to the
sequential loop from todo 11.  This means the same compute.jl script is
valid in three modes:

1. No `init_workers!` call at all → sequential on the master.
2. `init_workers!(mode=:distributed)` with `addprocs(n)` → local pmap fan-out.
3. `init_workers!(mode=:slurm)` inside a SLURM job → cluster fan-out.

Multi-master locking (several separate julia processes writing to the
same vault) continues to work underneath either path because the lock
layer (DataVault's `.running`) uses POSIX `link()` / atomic `rename` only.
"""
function run!(
    work_fn::Function, vault::Vault, keys::AbstractVector{DataKey}; opts::RunOpts=RunOpts()
)
    stage = Symbol(vault.run)
    log_name = "events_$(gethostname())_$(getpid()).jsonl"
    log = EventLog(joinpath(vault.outdir, log_name); min_level=opts.log_level)

    # Early skip: load manifest, subtract completed keys
    m = load_manifest(vault)
    todo = todo_keys(m, collect(keys))

    if isempty(todo)
        log_event(log, :skip_complete; stage=stage, total=length(keys))
        return (stage=stage, done=0, err=0, skipped=length(keys), total=length(keys))
    end

    log_event(log, :stage_start; stage=stage, total=length(keys), todo=length(todo))

    # Dispatch strategy: pmap when Distributed workers are present (unless the
    # caller forced `workers=:sequential`), otherwise the sequential loop.
    outcomes = if opts.workers !== :sequential && nprocs() > 1
        _run_pmap!(work_fn, vault, todo, stage, log, opts)
    else
        _run_sequential!(work_fn, vault, todo, stage, log, opts)
    end

    # Aggregate outcomes into counters + manifest updates.
    n_done = 0
    n_err = 0
    n_busy = 0
    n_gave_up = 0
    n_stop = 0
    for (key, outcome) in outcomes
        if outcome === :lock_busy
            n_busy += 1
        elseif outcome === :already_done
            add_complete!(m, key)
        elseif outcome === :ok
            add_complete!(m, key)
            n_done += 1
        elseif outcome === :stop
            n_stop += 1
        elseif outcome === :gave_up
            n_gave_up += 1
            n_err += 1
        else  # :error
            n_err += 1
        end
    end

    # Persist the updated manifest, merging with on-disk state so that
    # concurrent masters don't overwrite each other's completed keys.
    merge_and_save_manifest!(m)

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
    )
end

"""
    _run_one_with_lock!(work_fn, vault, key, stage, log, opts) -> (DataKey, Symbol)

Execute the per-key pipeline: atomic-acquire via
`DataVault.acquire_running!`, re-check completion, run work_fn with a
background heartbeat task, and release on exit.  Returns a
`(key, outcome)` pair suitable for aggregation by the caller.

Outcome symbols:
- `:lock_busy`    — another master holds a fresh `.running`, skipped.
- `:already_done` — finished by a sibling master between the manifest
                    read and the lock acquisition.
- `:ok`           — `work_fn` succeeded and `mark_done!` was called.
- `:error`        — single-attempt failure (`opts.max_attempts == 1`).
- `:gave_up`      — all `opts.max_attempts` attempts failed.
- `:stop`         — stop flag detected before work started.
"""
function _run_one_with_lock!(
    work_fn::Function,
    vault::Vault,
    key::DataKey,
    stage::Symbol,
    log::EventLog,
    opts::RunOpts,
)
    kstr = canonical(key)

    # Early exit if stop flag has been raised (checked by both sequential
    # and pmap paths, so each worker can bail independently).
    if _is_stopped(opts)
        return (key, :stop)
    end

    # DataVault owns the lock file.  `acquire_running!` is atomic on
    # NFS via POSIX `link()`: concurrent masters see at most one
    # `:ok` / `:reclaimed`; the losers see `:busy`.
    acq = DataVault.acquire_running!(vault, key; stale_after=opts.stale_after)
    if acq === :busy
        log_event(log, :lock_busy; level=:debug, stage=stage, key=kstr)
        return (key, :lock_busy)
    end
    # acq ∈ (:ok, :reclaimed) — we own the lock.

    # Re-check after acquisition: another master may have finished this
    # key between our manifest read and our acquire.
    if DataVault.is_done(vault, key)
        DataVault.clear_running!(vault, key)
        return (key, :already_done)
    end

    # Background heartbeat task: periodically refresh `.running` so
    # `DataVault.cleanup_stale` / sibling `acquire_running!` callers
    # recognise us as alive.  The `Threads.Atomic{Bool}` flag makes
    # the stop signal thread-safe; a short sleep tick keeps finally
    # cleanup responsive (the earlier fixed 60-s sleep would block the
    # whole shutdown until the next heartbeat tick).
    # `lost[]` is raised by the heartbeat task if it observes that a sibling
    # reclaimed our lock (we stalled past `stale_after`). `_run_one_with_retry!`
    # checks it before `save!`, and the `finally` below skips `clear_running!`
    # when lost, so we neither commit on top of nor delete the lock now owned by
    # the reclaiming master. Detection is BEST-EFFORT: `refresh_running!` is
    # existence-based, so a reclaim is reliably caught only via stale-reclaim or
    # the brief window the lock file is absent. A fully race-free guarantee needs
    # owner-stamped locks in DataVault (see PR notes).
    hb_stop = Threads.Atomic{Bool}(false)
    lost = Threads.Atomic{Bool}(false)
    hb_task = Threads.@spawn begin
        tick = 0.1
        elapsed = 0.0
        while !hb_stop[]
            sleep(tick)
            hb_stop[] && break
            elapsed += tick
            if elapsed >= opts.heartbeat_interval
                # A `false` return (sibling reclaimed) OR a throw (un-refreshable
                # lock, e.g. NFS hiccup) both mean "treat as lost": stop
                # heartbeating and signal it, rather than silently dying.
                alive = try
                    DataVault.refresh_running!(vault, key)
                catch
                    false
                end
                if !alive
                    lost[] = true
                    break
                end
                elapsed = 0.0
            end
        end
    end

    outcome = try
        _run_one_with_retry!(work_fn, vault, key, kstr, stage, log, opts, lost)
    finally
        hb_stop[] = true
        try
            wait(hb_task)
        catch
        end
        # Release the lock so the key is immediately retriable by a sibling
        # without waiting for `stale_after` — BUT NOT if we lost it. When
        # `lost[]`, the `.running` file is now the RECLAIMING master's, and
        # `clear_running!` is owner-blind (`isfile && rm`), so clearing it would
        # delete THEIR lock and re-open double-execution. On `:ok`, `mark_done!`
        # already removed our `.running`; `clear_running!` is otherwise idempotent.
        lost[] || DataVault.clear_running!(vault, key)
    end

    return (key, outcome)
end

"""
    _run_sequential!(work_fn, vault, todo, stage, log, opts) -> Vector{Tuple{DataKey,Symbol}}
"""
function _run_sequential!(
    work_fn::Function,
    vault::Vault,
    todo::AbstractVector{DataKey},
    stage::Symbol,
    log::EventLog,
    opts::RunOpts,
)
    results = Vector{Tuple{DataKey,Symbol}}()
    for key in todo
        if _is_stopped(opts)
            break
        end
        push!(results, _run_one_with_lock!(work_fn, vault, key, stage, log, opts))
    end
    return results
end

"""
    _run_pmap!(work_fn, vault, todo, stage, log, opts) -> Vector{Tuple{DataKey,Symbol}}

Fan `todo` out across `WorkerPool(workers())` via `pmap`.  Each worker
invokes `_run_one_with_lock!`, which serialises the per-key lock +
`work_fn` + save pipeline on that worker.  Returns the list of
`(key, outcome)` pairs for master-side aggregation.

Failures inside `work_fn` are already caught by `_run_one_with_retry!`
and turned into `(key, :gave_up)` / `(key, :error)`; we additionally set
`pmap`'s `on_error = identity` so an unexpected thrown exception bubbles
up as an `Exception` value in the outcomes vector rather than bringing
down the whole fan-out, and we log + convert those to `:error`. A worker
that *dies* mid-key (`ProcessExitedException`, e.g. SLURM preemption) is
re-dispatched to a live worker via `retry_check`.
"""
function _run_pmap!(
    work_fn::Function,
    vault::Vault,
    todo::AbstractVector{DataKey},
    stage::Symbol,
    log::EventLog,
    opts::RunOpts,
)
    pool = WorkerPool(workers())
    # `retry_check` re-dispatches a key whose worker DIED
    # (`ProcessExitedException`) to a live worker; `work_fn` errors are handled
    # inside `_run_one_with_lock!` and never escape, so they are not retried.
    raw = pmap(
        pool,
        todo;
        on_error=identity,
        retry_delays=ExponentialBackOff(; n=2),
        retry_check=(s, e) -> (s, e isa ProcessExitedException),
    ) do key
        return _run_one_with_lock!(work_fn, vault, key, stage, log, opts)
    end

    # Normalise any thrown exceptions back to (key, :error) tuples.
    out = Vector{Tuple{DataKey,Symbol}}(undef, length(todo))
    @inbounds for i in eachindex(todo)
        item = raw[i]
        if item isa Tuple{DataKey,Symbol}
            out[i] = item
        else
            # `pmap(; on_error = identity)` returns the exception value at
            # this slot.  Log it and mark the key as :error.
            kstr = canonical(todo[i])
            err = _short_err(item)
            log_event(log, :error; stage=stage, key=kstr, attempt=0, err=err)
            out[i] = (todo[i], :error)
        end
    end
    return out
end

"""
    _run_one_with_retry!(work_fn, vault, key, kstr, stage, log, opts, lost) -> Symbol

Execute `work_fn(key)` up to `opts.max_attempts` times. Returns:
  :ok        — payload saved and mark_done! called
  :gave_up   — all attempts failed, final `:gave_up` event logged
  :error     — single-attempt config (`max_attempts == 1`) that failed once
  :lock_busy — `lost[]` was set (a sibling reclaimed our lock); the result is
               discarded before `save!` so the reclaiming master's result wins
"""
function _run_one_with_retry!(
    work_fn,
    vault::Vault,
    key::DataKey,
    kstr::String,
    stage::Symbol,
    log::EventLog,
    opts::RunOpts,
    lost::Threads.Atomic{Bool},
)
    last_err = nothing
    for attempt in 1:opts.max_attempts
        log_event(log, :key_start; level=:debug, stage=stage, key=kstr, attempt=attempt)
        t0 = time()
        try
            payload = work_fn(key)
            payload isa Dict || error(
                "work_fn must return a Dict (got $(typeof(payload))). " *
                "Wrap scalars as e.g. Dict(\"value\" => x).",
            )
            if lost[]
                # A sibling master reclaimed our lock while work_fn ran; it now
                # owns this key. Discard our result rather than double-committing.
                log_event(log, :lock_lost; stage=stage, key=kstr, attempt=attempt)
                return :lock_busy
            end
            DataVault.save!(vault, key, payload)
            DataVault.mark_done!(vault, key)
            log_event(
                log, :key_done; stage=stage, key=kstr, secs=time() - t0, attempt=attempt
            )
            return :ok
        catch e
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
    run_loop!(work_fn, vault, keys; opts=RunOpts(),
              max_empty_rounds=3, idle_sleep=30.0)

Work-stealing loop that repeatedly calls [`run!`](@ref) until there is no
more work to do. This is the infra equivalent of FiniteTemperature.jl's
`_work_loop` driver.

The loop exits when:
- `max_empty_rounds` consecutive rounds produce zero new completions, or
- `opts.stop_flag` is raised (graceful shutdown).

Default parameters (`max_empty_rounds=3`, `idle_sleep=30.0`) are the
battle-tested values from FiniteTemperature.jl.
"""
function run_loop!(
    work_fn::Function,
    vault::Vault,
    keys::AbstractVector{DataKey};
    opts::RunOpts=RunOpts(),
    max_empty_rounds::Int=3,
    idle_sleep::Float64=30.0,
)
    empty_count = 0
    while true
        if _is_stopped(opts)
            break
        end
        result = run!(work_fn, vault, keys; opts=opts)
        if result.done > 0
            empty_count = 0
            continue
        end
        empty_count += 1
        if empty_count >= max_empty_rounds
            break
        end
        sleep(idle_sleep)
    end
    return nothing
end

export RunOpts, run!, run_loop!, manifest_root, load_manifest
