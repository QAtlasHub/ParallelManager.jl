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
Concurrent writes from multiple tasks (within one master) are serialized
through an internal `ReentrantLock`. Concurrent writes from multiple
_processes_ (separate masters) rely on POSIX `O_APPEND` atomicity, which
is guaranteed for single `write` syscalls of length `< PIPE_BUF` (4 KiB);
`log_event` composes each line as a single `String` and issues exactly
one `write(io, line)` call to stay within that guarantee.

# Fields

- `path::String` — target JSONL file. Parent directory is created lazily on
  first [`log_event`](@ref).
- `lock::ReentrantLock` — protects appends from same-process races.

# Event kinds used by `run!`

[`run!`](@ref) emits the following `kind` values (as strings in the JSON):

| kind            | when                                                              |
| :-------------- | :---------------------------------------------------------------- |
| `stage_start`   | once at the top of `run!` when `todo` is non-empty                |
| `stage_done`    | once at the bottom of `run!` when `todo` was non-empty            |
| `key_start`     | before each `work_fn(key)` attempt (includes `attempt` field)     |
| `key_done`      | after a successful `work_fn(key)` (includes `secs`, `attempt`)    |
| `lock_busy`     | `KeyLock.try_acquire` returned `false`                            |
| `lock_reclaimed`| (reserved, not currently emitted)                                 |
| `error`         | `work_fn` threw on this attempt                                   |
| `retry`         | another attempt will follow                                       |
| `gave_up`       | all `max_attempts` attempts exhausted                             |
| `skip_complete` | full-done early exit (manifest had every key)                     |

Downstream analysis (`jq`, DataFrame-based) can filter and aggregate over
these kinds without ever parsing freeform text.

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
end

EventLog(path::AbstractString) = EventLog(String(path), ReentrantLock())

"""
    log_event(log, kind; kwargs...)

Append one JSON object to `log.path` with fields `ts` (ISO-8601 local time),
`kind` (the `Symbol` converted to `String`), and any additional key/value
pairs passed via `kwargs`.

The line is built in full (including the trailing newline) as a single
`String`, then written with exactly one `write(io, line)` call inside an
`open(path, "a")` block. This relies on POSIX `O_APPEND` atomicity so that
cross-process writes do not tear each other's lines.

```julia
log_event(log, :key_done; stage=:phase1, key="N=8;J=1.0;#sample=1", secs=12.3)
```

produces one line like:

```json
{"ts":"2026-04-13T14:23:51.123","kind":"key_done","stage":"phase1","key":"N=8;J=1.0;#sample=1","secs":12.3}
```

Returns `nothing`.
"""
function log_event(log::EventLog, kind::Symbol; kwargs...)
    rec = (; ts=string(now()), kind=String(kind), kwargs...)
    # Build the full line with newline so a single `write` is one atomic
    # append on POSIX (given `O_APPEND` and size < PIPE_BUF).
    line = string(JSON3.write(rec), '\n')
    lock(log.lock) do
        mkpath(dirname(log.path))
        open(log.path, "a") do io
            return write(io, line)
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
