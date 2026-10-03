# EventLog — structured JSONL event logging.
#
# Per-item `println` logging is a non-goal and is not part of the public API.
# FiniteTemperature.jl used to emit ~300 MB of job logs by printing a line
# for every one of 3600 keys; switching to aggregated events collapses that
# to ~1 event per key plus a handful of per-stage events.

using Dates
using JSON3

"""
    EventLog(path::AbstractString)

Append-only JSONL event log with a thread-safe per-`EventLog` lock.

Each call to [`log_event`](@ref) writes one JSON object as a single line.
Concurrent writes from multiple tasks are serialized through a
`ReentrantLock` held per PATH, so several `EventLog` objects on one file
share it. Concurrent writes from multiple _processes_ (separate masters)
rely on POSIX `O_APPEND` atomicity, which is guaranteed for single `write`
syscalls of length `< PIPE_BUF` (4 KiB); `log_event` composes each line as
a single `String` and issues one `write` to an unbuffered descriptor to
stay within that guarantee.

# Fields

- `path::String` — target JSONL file. Parent directory is created lazily on
  first [`log_event`](@ref).
- `lock::ReentrantLock` — the per-path lock at construction time. [`log_event`](@ref) resolves the
  lock from `path` rather than reading this field, so an `EventLog` that arrived on a worker by
  deserialization (which skips the constructor) still serialises against its siblings there.

# Event kinds used by `run!`

[`run!`](@ref) emits the following `kind` values (as strings in the JSON):

| kind            | when                                                              |
| :-------------- | :---------------------------------------------------------------- |
| `stage_start`   | once at the top of `run!` when `todo` is non-empty                |
| `stage_done`    | once at the bottom of `run!` when `todo` was non-empty: the        |
|                 | round's totals, incl. `held_back` and `collisions` (keys handed    |
|                 | out that another master had taken), and where its wall time went:  |
|                 | `prepare_secs`, `scan_secs`, `dispatch_secs`, `manifest_secs`,     |
|                 | `total_secs`                                                       |
| `key_acquired`  | the per-key lock was taken (includes `acq`); the only durable       |
|                 | record of a claim, since a SIGKILL skips every later event         |
| `key_start`     | before each `work_fn(key)` attempt (includes `attempt` field)     |
| `key_done`      | after a successful `work_fn(key)`: `secs` (wall), `cpu` (CPU       |
|                 | seconds), `cores`, `rss` (peak bytes), `host`, `class`, `attempt`, |
|                 | `sha256`, and `note` (what `work_fn` added with `note_key!`)       |
| `key_spent`     | an attempt that did not finish its key, and what it cost:          |
|                 | `outcome` (`error`, `stopped`, `lock_lost`, `worker_died`), `secs`,|
|                 | `cpu`, `cores`, `rss`, `rss_scope`, `host`, `class`                |
| `cost_source`   | `run!` took its cost from the measured table (`classes`,           |
|                 | `measured_keys`, `fallback_keys`, `fallback`)                      |
| `cost_table_unreadable` | the stage's cost table could not be read; the caller's     |
|                 | hook is used                                                       |
| `lock_busy`     | another master holds the `.running` lock: found by the master's    |
|                 | scan before dispatch, or by a worker's acquire (= `:busy`)         |
| `lock_lost`     | our lock was reclaimed mid-work; result discarded (no double-run) |
| `lock_reaped`   | a lock whose holder was shown dead was cleared without waiting     |
|                 | (includes `owner`, and `why`: the evidence)                        |
| `locks_reconciled` | the master's pass over the locks before it built its queue, when |
|                 | there was any (includes `locks`, `held`, `held_jobs`, `reaped`,    |
|                 | `dead_jobs`, `stale`, `unknown`)                                   |
| `reap_failed`   | reaping threw; the key falls back to the `stale_after` timeout     |
| `lock_released` | the master took back a lock it had named (includes `why`:          |
|                 | `worker_exited`, or `master_exit` for a key cut when it left)      |
| `lock_reclaimed`| (reserved, not currently emitted)                                 |
| `error`         | `work_fn` threw on this attempt                                   |
| `retry`         | another attempt will follow                                       |
| `gave_up`       | all `max_attempts` attempts exhausted                             |
| `skip_complete` | full-done early exit (manifest had every key)                     |
| `worker_died`   | the worker exited on this key every time it was dispatched, up to |
|                 | the re-dispatch bound (includes `deaths`)                          |
| `worker_lost`   | every worker died with keys still queued; this key was left for a  |
|                 | later run rather than completed or failed                          |
| `campaign_start`| [`run_campaign!`](@ref) began: the meta file, its `sha256`, the    |
|                 | `profile`, the `stages` in order (in `events_campaign_*.jsonl`)    |
| `campaign_stage`| one stage of a campaign: `ran` or the `reason` it did not, its     |
|                 | key count and its `run_loop!` totals                               |
| `campaign_reloaded` / `campaign_reload_refused` | the meta file changed under a        |
|                 | running campaign and was taken up, or was broken and ignored       |
| `campaign_done` | the campaign returned (`stages`, `ran`, `stopped_by`)              |
| `pool_spawn`    | a [`SizedPool`](@ref) started workers of one size on a node        |
|                 | (`node`, `cores`, `mem_gb`, `n`); `pool_spawn_failed` when it      |
|                 | could not (`err`)                                                  |
| `pool_limit`    | the most workers the pool will hold and where that number came     |
|                 | from (`max_workers`, `source`); once per pool                      |
| `pool_at_limit` | a start was wanted past that limit (`held`, `queued`); once        |
| `pool_spawn_short` | a start brought fewer workers than asked (`asked`, `started`)   |
| `pool_stalled`  | starts have neither joined nor failed for `stall_after`            |
| `pool_gave_up`  | ten starts failed in a row with keys still queued; `run!` throws   |
| `pool_retire`   | an idle worker whose size no queued key fits gave its room back    |
| `pool_retry_mem`| a worker died under a key: the key is retried with more memory     |
|                 | (`had_gb`, `next_gb`)                                              |
| `key_too_big`   | a key needs more than any node offers, and is reported, not        |
|                 | retried (`cores`, `mem_gb`, `node_cores`, `node_mem_gb`)           |
| `held_back`     | keys this job did not start because they could not get anywhere    |
|                 | before its deadline (`keys`, `secs_left`); once per round          |
| `job_account`   | when a master ends: where its core-seconds went (`account`:        |
|                 | `allocated`, `computing`, `kept`, `lost`, `keys_cut`, `startup`,   |
|                 | `never_started`, `idle` by reason, `other`)                        |
| `job_decision`  | what job management concluded for a partition: `action` (`submit`, |
|                 | `hold`, `refuse`), `reason`, `node_hours`, `dry_run` (in           |
|                 | `events_jobs_*.jsonl`)                                             |
| `job_submitted` | a job was submitted (`id`, `partition`, `nodes`, `node_hours`)     |
| `underused`     | the queue was empty and too few workers had a unit for             |
|                 | `idle_grace`: the master stopped on purpose (`busy`, `workers`)    |
| `control_request` | a [`control!`](@ref) request was applied (includes `id`, `op`,   |
|                 | `by`: who asked, `asked_at`, and `detail`: what it changed)        |
| `control_not_applied` | a request this master could not carry out (`detail` has      |
|                 | `error` or `unsupported`); `:warn`                                 |
| `control_bad_request` | a request file that could not be read after three tries      |
| `control_ack_failed` | the acknowledgement of a request could not be written         |
| `release_failed`| a lock this master meant to release is still there (`err`)         |
| `checkpoint_unreadable` | a key's checkpoint could not be read: kept aside (`kept`), |
|                 | the key starts over                                                |
| `progress_unreadable` | progress stamps that could not be read (`files`)             |
| `status_write_failed` | the status file could not be written; once per run of        |
|                 | failures                                                           |
| `cost_table_failed` | the per-class cost table could not be written                  |
| `key_stopped`   | a unit told to stop left at a safe point ([`stop_point`](@ref));   |
|                 | no attempt spent                                                   |
| `key_cut`       | a unit told to stop was still running after its grace: its worker  |
|                 | was removed, then its lock released (`worker_removed`,             |
|                 | `lock_released`, `request`; `:warn`)                               |
| `worker_retired`| a `:resize` took a worker out of the pool, between units           |
| `workers_joined`| workers that joined after the round began were adopted (`n`)       |
| `workers_rejected` | workers that joined late could not be readied and get no work   |
| `workers_short` | fewer workers joined than `note_workers!` said were planned, for   |
|                 | longer than the worker timeout (includes `planned`, `launched`,    |
|                 | `joined`); logged once per distinct shortfall, at `:warn`          |
| `artifact_busy` | `work_fn` threw `DataVault.ArtifactBusy`; the key is deferred, no  |
|                 | attempt spent (includes `artifact`)                                |
| `deferred_round`| `run!` re-dispatches its deferred keys (includes `round`, `keys`) |

`:key_start` and `:lock_busy` are emitted at `:debug` level and are suppressed
unless the `EventLog` is created with `min_level=:debug` (see `RunOpts.log_level`);
their totals still appear in `:stage_done`. Downstream analysis (`jq`,
DataFrame-based) can filter and aggregate over these kinds without ever parsing
freeform text.

# Example

```julia
log = EventLog("out/events.jsonl")
log_event(log, :stage_start; stage=:phase1, todo=3600)
# ... work ...
log_event(log, :stage_done; stage=:phase1, done=3600, err=0)
```
"""
struct EventLog
    path::String
    lock::ReentrantLock
    min_level::Int
end

function EventLog(path::AbstractString; min_level::Symbol=:info)
    return EventLog(String(path), _path_lock(path), _level_value(min_level))
end

# One lock per PATH, process-wide, NOT one per `EventLog`. `run!` builds a fresh `EventLog` on every
# call, so four concurrent masters in one process hold four objects pointing at one file and a
# per-object lock serialises nothing between them.
const _LOG_LOCKS = Dict{String,ReentrantLock}()
const _LOG_LOCKS_GUARD = ReentrantLock()

function _path_lock(path::AbstractString)::ReentrantLock
    key = abspath(String(path))
    return lock(_LOG_LOCKS_GUARD) do
        return get!(ReentrantLock, _LOG_LOCKS, key)
    end
end

# Severity ladder (à la Julia logging). Events below an `EventLog`'s `min_level`
# are dropped — used to keep high-churn `:debug` events (per-key `:lock_busy`,
# `:key_start`) out of the log by default while preserving the aggregate counts
# carried in `:stage_done`.
function _level_value(l::Symbol)::Int
    l === :debug && return 10
    l === :info && return 20
    l === :warn && return 30
    l === :error && return 40
    return throw(
        ArgumentError("EventLog: unknown level $(repr(l)) (use :debug/:info/:warn/:error)")
    )
end

"""
    log_event(log, kind; kwargs...)

Append one JSON object to `log.path` with fields `ts` (ISO-8601 local time),
`kind` (the `Symbol` converted to `String`), and any additional key/value
pairs passed via `kwargs`.

The line is built in full (including the trailing newline) as a single
`String` and written with one `write` syscall to an UNBUFFERED append-mode
descriptor. Both halves matter: the syscall is what POSIX `O_APPEND`
atomicity applies to, so cross-process writes do not tear each other's
lines, and an `IOStream` would flush on its own boundaries instead.

```julia
log_event(log, :key_done; stage=:phase1, key="N=8;J=1.0;#sample=1", secs=12.3)
```

produces one line like:

```json
{"ts":"2026-04-13T14:23:51.123","kind":"key_done","stage":"phase1","key":"N=8;J=1.0;#sample=1","secs":12.3}
```

Returns `nothing`.
"""
function log_event(log::EventLog, kind::Symbol; level::Symbol=:info, kwargs...)
    # Drop events below the log's threshold. `level` is consumed here (a filter
    # decision); it is NOT written into the JSON — the `kind` already implies it.
    _level_value(level) < log.min_level && return nothing
    rec = (; ts=string(now()), kind=String(kind), kwargs...)
    # Build the full line with newline so a single `write` is one atomic
    # append on POSIX (given `O_APPEND` and size < PIPE_BUF).
    line = string(JSON3.write(rec), '\n')
    # Resolved from the PATH, not taken from `log.lock`. `run!` serialises the `EventLog` to every
    # worker, and deserialization rebuilds the struct without running the constructor, so the field
    # that arrives on a worker is a private lock that serialises nothing against its siblings.
    lock(_path_lock(log.path)) do
        mkpath(dirname(log.path))
        fd = Base.Filesystem.open(
            log.path,
            Base.Filesystem.JL_O_WRONLY | Base.Filesystem.JL_O_CREAT |
            Base.Filesystem.JL_O_APPEND,
            0o644,
        )
        try
            return write(fd, codeunits(line))
        finally
            close(fd)
        end
    end
    return nothing
end

"""
    merge_event_logs(dir; output="events_merged.jsonl") -> String

Merge all per-master event log files (`events_*.jsonl`) in `dir` into a
single sorted file. Returns the output path.

Each master writes to its own `events_<host>_<pid>.jsonl`; this function
collects and sorts all lines by their `ts` field for post-hoc analysis.
"""
function merge_event_logs(dir::AbstractString; output::String="events_merged.jsonl")
    logs = filter(readdir(dir)) do f
        return startswith(f, "events_") && endswith(f, ".jsonl") && f != output
    end
    all_lines = String[]
    for f in logs
        append!(all_lines, readlines(joinpath(dir, f)))
    end
    sort!(all_lines; by=l -> JSON3.read(l).ts)
    outpath = joinpath(dir, output)
    open(outpath, "w") do io
        for l in all_lines
            println(io, l)
        end
    end
    return outpath
end

export EventLog, log_event, merge_event_logs
