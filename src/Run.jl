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
#   0.6.9: the master holds the task table (TaskTable.jl) and hands keys out itself, with the lock
#       token and the resume point; `pmap` is gone.

using Distributed
using DataVault
using ParamIO: DataKey, canonical

"""
    RunOpts(; workers=:auto, max_attempts=3, stale_after=600.0, heartbeat_interval=60.0,
             stop_flag=ENV["SWEEPRUNNER_STOP_FLAG"], log_level=:info, deadline=nothing,
             defer_poll=30.0, status_interval=60.0, control_interval=10.0,
             min_busy_fraction=0.0, idle_grace=600.0, checkpoint_every=600.0,
             stop_grace=Inf, shard=<env>, order=:given, manifest_interval=300.0,
             stuck_after=0.0)

Execution options for [`run!`](@ref).

# Fields

- `workers::Symbol = :auto` — dispatch mode. `:auto` fans out over the
  Distributed `workers()` when `nprocs() > 1`, and runs
  sequentially otherwise. `:sequential` forces the sequential path even when
  worker processes are present (useful for debugging a serialization issue).
- `max_attempts::Int = 3` — per-key retry budget. Set to `1` to disable
  retry (a failed `work_fn` is logged as `:error` instead of `:gave_up`).
- `stale_after::Float64 = 600.0` — the heartbeat age past which a lock NOBODY ANSWERS FOR is
  reclaimed. It is the last resort, not the usual wait: a lock a reporting master lists as held
  is left alone whatever its age, and a lock whose holder is known to be gone (its master no
  longer lists it, its job ended, its pid is not there) is removed when a job starts, without
  waiting. Passed through to `DataVault.acquire_running!`.
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
  RunOpts(deadline_in = 25 * 60)         # the same, said as seconds from now
  ```

  `deadline_in` is the relative form (seconds from when the options are built); give one or the
  other. A `deadline` too small to be a `time()` is warned about: it is already past.

The numbers are checked when the options are built: `max_attempts >= 1`; `stale_after`,
`heartbeat_interval` and `defer_poll` positive; the intervals and graces non-negative (`0` is
"off" where the field says so); `min_busy_fraction` in `[0, 1]`.

- `defer_poll::Float64 = 30.0` — seconds [`run!`](@ref) waits before re-dispatching keys whose
  `work_fn` threw `DataVault.ArtifactBusy` (an artifact being built by another worker or job),
  when the previous pass made no progress. A deferred key costs no attempt.
- `status_interval::Float64 = 60.0` — how often the master rewrites its status file (see
  [`read_status`](@ref)): task counts, the worker pool against what was planned, and per worker
  the key it is on, CPU utilisation and RSS. `0` writes none.
- `control_interval::Float64 = 10.0` — how often the master looks for [`control!`](@ref)
  requests (add work, cancel, stop, prioritise, resize, drain, pause) and for workers that joined
  since the round began. `0` takes no requests.
- `min_busy_fraction::Float64 = 0.0`, `idle_grace::Float64 = 600.0` — leave on purpose. When
  the queue is empty and less than this fraction of the workers' cores has had a unit for
  `idle_grace` seconds, the master stops: the job is holding its nodes for a few long units.
  The units still running are told to stop (they leave at their next [`stop_point`](@ref), with
  their progress recorded), and `run!` returns `stopped_by = :underused`, so the allocation is
  given back instead of being held to the wall clock. `0` (the default) never does.
- `checkpoint_every::Float64 = 600.0` — how often [`checkpoint_due`](@ref) says it is time to
  save, inside a `work_fn` that keeps a checkpoint ([`save_checkpoint!`](@ref)). It is also due
  at once on a stop and when the `deadline` is close. `0`: only then.
- `stop_grace::Float64 = Inf` — a bound on the job's own stop. Once `stop_flag` is raised or
  `deadline` has passed, a unit still running after this many seconds is cut: its worker is
  removed and its lock released, so `run!` returns. Units that call [`stop_point`](@ref) leave
  before that with their checkpoint. `Inf` (the default) waits for running units, as before;
  set it below the lead your scheduler gives before it kills the job.
- `shard::Union{Tuple{Int,Int},Nothing}` — `(i, m)`: this is master `i` of `m` cooperating on one
  sweep (`0 <= i < m`). It starts on the keys whose hash falls in its share and reaches the
  others' only when its own run out, so masters that start together do not spend their first
  passes on each other's locks. Every master still covers every key: a share is where it
  STARTS, not all it does. **Defaults to `ENV["SWEEPRUNNER_SHARD"]`** written `i/m`, and to the
  Slurm array task (`SLURM_ARRAY_TASK_ID` of `SLURM_ARRAY_TASK_COUNT`) when the job is one.
- `order::Symbol = :given` — `:longest_first` hands out the keys with the largest `cost` first
  (see `cost` in [`run!`](@ref)), so the long keys start while there is time for them and the
  short ones fill what is left of the job. `:given` keeps the caller's order.
- `manifest_interval::Float64 = 300.0` — how often the keys finished so far are merged into the
  manifest WHILE the round runs. The manifest used to be written only when a round ended, so a
  job killed at its wall clock left none of its completions there and the next job found them
  again one marker at a time. `0` writes it at the end only.
- `stuck_after::Float64 = 0.0` — a running key that has reported no progress for this many
  seconds (since it started, or since its last [`report_progress`](@ref) /
  [`save_checkpoint!`](@ref)) is said to be stuck: a `key_stuck` warning in the event log, once
  per key, and `stuck` on its worker's row in the status, with a line in the warnings. Nothing
  is cut — a heartbeat only shows the process is alive, and this is the missing half, "alive
  and not advancing", left for a person or a [`control!`](@ref) request to act on. Set it above
  the longest step your `work_fn` takes between two reports. `0` (the default) never says it.

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
    control_interval::Float64
    min_busy_fraction::Float64
    idle_grace::Float64
    checkpoint_every::Float64
    stop_grace::Float64
    shard::Union{Tuple{Int,Int},Nothing}
    order::Symbol
    manifest_interval::Float64
    stuck_after::Float64
end

# `time()` was past this in 2001: an absolute deadline below it was meant as a duration.
const _DEADLINE_LOOKS_RELATIVE = 1.0e9

function RunOpts(;
    workers::Symbol=:auto,
    max_attempts::Int=3,
    stale_after::Real=600.0,
    heartbeat_interval::Real=60.0,
    stop_flag::Union{String,Nothing}=get(ENV, "SWEEPRUNNER_STOP_FLAG", nothing),
    log_level::Symbol=:info,
    deadline::Union{Real,Nothing}=nothing,
    deadline_in::Union{Real,Nothing}=nothing,
    defer_poll::Real=30.0,
    status_interval::Real=60.0,
    control_interval::Real=10.0,
    min_busy_fraction::Real=0.0,
    idle_grace::Real=600.0,
    checkpoint_every::Real=600.0,
    stop_grace::Real=Inf,
    shard::Union{Tuple{<:Integer,<:Integer},Nothing}=_shard_from_env(),
    order::Symbol=:given,
    manifest_interval::Real=300.0,
    stuck_after::Real=0.0,
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
    order in (:given, :longest_first) || throw(
        ArgumentError(
            "RunOpts: order must be :given or :longest_first, got $(repr(order))"
        ),
    )
    (shard === nothing || (shard[2] >= 1 && 0 <= shard[1] < shard[2])) || throw(
        ArgumentError("RunOpts: shard must be (i, m) with 0 <= i < m, got $(repr(shard))"),
    )
    max_attempts >= 1 || throw(
        ArgumentError(
            "RunOpts: max_attempts must be >= 1, got $max_attempts (with 0 a key is given " *
            "up on without one attempt)",
        ),
    )
    # `x > 0` and `x >= 0` are false for NaN too, so a NaN is refused with the rest.
    for (name, x) in (
        ("stale_after", stale_after),
        ("heartbeat_interval", heartbeat_interval),
        ("defer_poll", defer_poll),
    )
        x > 0 || throw(ArgumentError("RunOpts: $name must be > 0 seconds, got $x"))
    end
    for (name, x) in (
        ("status_interval", status_interval),
        ("control_interval", control_interval),
        ("manifest_interval", manifest_interval),
        ("checkpoint_every", checkpoint_every),
        ("idle_grace", idle_grace),
        ("stop_grace", stop_grace),
        ("stuck_after", stuck_after),
    )
        x >= 0 ||
            throw(ArgumentError("RunOpts: $name must be >= 0 seconds (0: off), got $x"))
    end
    0 <= min_busy_fraction <= 1 || throw(
        ArgumentError(
            "RunOpts: min_busy_fraction is a fraction of the workers, in [0, 1]; got " *
            "$min_busy_fraction",
        ),
    )
    if deadline_in !== nothing
        deadline === nothing || throw(
            ArgumentError(
                "RunOpts: give `deadline` (an absolute time()) or `deadline_in` (seconds " *
                "from now), not both",
            ),
        )
        deadline_in >= 0 ||
            throw(ArgumentError("RunOpts: deadline_in must be >= 0, got $deadline_in"))
        deadline = time() + deadline_in
    elseif deadline !== nothing
        isnan(deadline) && throw(ArgumentError("RunOpts: deadline is NaN"))
        # Every other option is seconds FROM NOW; this one is a point in time. A value that
        # small is a duration written in its place, and the run it gives stops at once.
        deadline < _DEADLINE_LOOKS_RELATIVE && @warn(
            "RunOpts: deadline = $deadline is an absolute time() and is long past, so the " *
                "run stops at once. For \"$deadline seconds from now\" write " *
                "`deadline_in = $deadline`.",
        )
    end
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
        Float64(control_interval),
        Float64(min_busy_fraction),
        Float64(idle_grace),
        Float64(checkpoint_every),
        Float64(stop_grace),
        shard === nothing ? nothing : (Int(shard[1]), Int(shard[2])),
        order,
        Float64(manifest_interval),
        Float64(stuck_after),
    )
end

# `i/m` from `SWEEPRUNNER_SHARD`, else the Slurm array task; `nothing` when neither says.
function _shard_from_env()
    s = get(ENV, "SWEEPRUNNER_SHARD", "")
    if !isempty(s)
        parts = split(s, '/')
        if length(parts) == 2
            i, m = tryparse(Int, parts[1]), tryparse(Int, parts[2])
            (i !== nothing && m !== nothing && m >= 1 && 0 <= i < m) && return (i, m)
        end
        return nothing
    end
    id = tryparse(Int, get(ENV, "SLURM_ARRAY_TASK_ID", ""))
    n = tryparse(Int, get(ENV, "SLURM_ARRAY_TASK_COUNT", ""))
    lo = something(tryparse(Int, get(ENV, "SLURM_ARRAY_TASK_MIN", "")), 0)
    (id === nothing || n === nothing || n < 2) && return nothing
    i = id - lo
    return 0 <= i < n ? (i, n) : nothing
end

# Why the loop is stopping, so `:stage_done` can say which of the two fired rather than leaving
# a reader to guess from the wall clock.
function _stop_reason(opts::RunOpts)::Union{Symbol,Nothing}
    opts.stop_flag !== nothing && isfile(opts.stop_flag) && return :flag
    opts.deadline !== nothing && time() > opts.deadline && return :deadline
    return nothing
end

# The same, for a master: a `control!(…, :stop)` with no scope stops it too. The job's own bounds
# outrank a request, so a stage cut by the wall clock is not attributed to whoever asked last.
function _stop_reason(opts::RunOpts, master::Master)::Union{Symbol,Nothing}
    r = _stop_reason(opts)
    r === nothing || return r
    return master.ctl.stop_all ? master.ctl.stop_why : nothing
end

# The per-key outcome vocabulary names the same reasons `_stop_reason` does, for a
# `(key, outcome)` tuple that sits alongside `:ok` / `:error`. Written once, and loudly: a third
# reason added above must fail here rather than be silently relabelled as a deadline.
function _stop_outcome(reason::Symbol)::Symbol
    reason === :flag && return :stop_flag
    reason === :deadline && return :stop_deadline
    reason === :request && return :stop_request
    reason === :underused && return :stop_request      # the master asked, of itself
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
         master=nothing, spawn=nothing, key_class=nothing, cost=nothing, min_time=nothing,
         pool=nothing) -> NamedTuple

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

A preference, not a partition: a worker is never idle while a key it may take is pending (with a
`pool`, a worker takes only keys of its size), so a 200-key group
does not serialise onto the worker that opened it. When a worker has nothing from its own groups
left it takes from the group with the most work outstanding, which spreads workers over groups.

Only affects the fan-out; the sequential path visits keys in order. Keys are drawn by group, so
`order=:longest_first` and a shard's order hold inside a group, not across groups.

# The task table

The master reads the markers once, before it dispatches: keys finished by a sibling since the
manifest was written are settled, a lock whose holder is provably gone is removed, and a key held
by a live sibling is not handed out at all (it is counted in `busy`). What is left is the queue.
Each key goes to a worker together with its lock token and the last progress recorded for it, so
`work_fn` can ask [`resume_point`](@ref) instead of probing its own outputs; see
[`report_progress`](@ref).

When the queue drains, the keys that came back busy are asked about once more, since their holder
may have finished or died while the pass ran.

`key_class` is `key -> label`: the class a key's cost is recorded under (a size, a model). Every
finished key leaves a `key_done` record with its wall time, CPU time, cores, peak memory and node;
[`key_costs`](@ref) reads them back and [`cost_summary`](@ref) groups them by this label, so the
next job can be sized from what keys of each class actually took. Inside `work_fn`,
[`note_key!`](@ref) adds fields to the record.

# Cost and the wall clock

`cost` is `key -> seconds`, the key's expected run time ([`measured_cost`](@ref) builds one from
what earlier keys took). With `RunOpts(order=:longest_first)` the queue is drawn longest first.

`min_time` is `key -> seconds`, how long the key needs to get somewhere: to its next checkpoint,
or to its end if it keeps none (it defaults to `cost`). With a `deadline`, a key that needs more
than the time left is NOT started: a key that is run, cut at the wall clock and started again
from the same point by the next job occupies a worker for nothing. Such keys are counted in
`held_back`, logged once (`held_back`), and left for a job with more time. The check is made each
time a key is handed out, so as the job runs down the long keys are passed over and the short ones
still fill it.

Without a `min_time` it is `cost`, the whole key — except for a key with a progress record
(an earlier attempt called `save_checkpoint!` or `report_progress`): that one needs only
`opts.checkpoint_every` to get somewhere, so a checkpointing key longer than the job is still
advanced by it. A key with no record yet is held to its whole time.

# Workers sized to their keys

`pool` is a [`SizedPool`](@ref): instead of `n` identical workers started by `init_workers!`, the
pool starts a worker of the size a key needs (`key_req`), where a node has the room, as the queue
is drawn, and a worker only takes the keys it can hold. A worker that dies under a key has the
key retried with more memory.

`spawn` is `n -> start n more workers`: what a `:resize` request calls to grow the pool. Without
it the pool can only shrink.

`master` is the [`Master`](@ref) this call runs as (a fresh one by default). A caller that makes
several `run!` calls as one job (as [`run_loop!`](@ref) does) passes the same one to each, so they
share an event log, a status file and the worker identities already collected.

# Control

While it runs, the master takes requests ([`control!`](@ref)): add keys, cancel or stop part of
the work, move keys to the front, resize the pool, drain a node, pause. They are applied every
`opts.control_interval` seconds, and each one is logged with who asked. Workers that join after
the round began are adopted at the same cadence. Inside `work_fn`, [`should_stop`](@ref) /
[`stop_point`](@ref) are where a unit that was told to stop leaves.

# Status

While it runs, the master rewrites `<state_root>/masters/<id>/status.json` every
`opts.status_interval` seconds; [`read_status`](@ref) / [`print_status`](@ref) read it from any
process, during the job or after it.

Returns `(; stage, done, err, busy, gave_up, stop, cancelled, held_back, collisions, skipped,
total, remaining, stopped_by)`. `cancelled` counts the keys a request took out of this job;
`held_back` the keys not started because they could not get anywhere before the deadline;
`collisions` the keys that were handed to a worker and came back because another master took
them first; `total` includes the
keys a request added; `remaining` is how many keys are not done after the round (`0`: the sweep
is complete). `stopped_by` is `:flag`, `:deadline`, `:request`, `:underused`, or `nothing`: a stage that finished every key reports `nothing` even if the
deadline passed while its last key ran, since no key was ever held back by it.
The full-done early exit returns the same field set rather than a shorter one.

Contract:
- `work_fn` is expected to be a pure function: given a `DataKey`, return a
  `Dict` payload to persist via `DataVault.save!`.
- Exceptions in `work_fn` are caught and logged; the corresponding key's
  `.done` file is not written, so re-runs will pick it up.
- The stage label used for logging is `Symbol(vault.run)`.
- Manifest is monotonic: newly completed keys are merged in every `opts.manifest_interval`
  seconds while the round runs, and once more when it ends.

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
    spawn=nothing,
    key_class=nothing,
    cost=nothing,
    min_time=nothing,
    pool=nothing,
)
    stage = Symbol(vault.run)
    # A master handed in outlives this call (`run_loop!` between rounds); one made here does not.
    own = master === nothing
    master = own ? Master() : master
    log_name = "events_$(master.id).jsonl"
    log = EventLog(joinpath(vault.outdir, log_name); min_level=opts.log_level)
    # A pool starts its own workers as the queue needs them, so there may be none yet.
    multi = opts.workers !== :sequential && (nprocs() > 1 || pool !== nothing)
    master.vault = vault
    master.stage = String(stage)
    master.multi = multi
    master.interval = opts.status_interval
    master.stuck_after = opts.stuck_after
    spawn === nothing || (master.ctl.spawn = spawn)
    after = own ? :ended : :waiting
    # Requests made while no round was running (between rounds, or just before this call), and
    # the keys earlier requests added: they are part of the sweep from here on.
    poll_control!(master, nothing, log, opts; force=true)
    keys = _with_extra(keys, master.ctl.extra)

    t_run = time()
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
            cancelled=0,
            held_back=0,
            collisions=0,
            skipped=length(keys),
            total=length(keys),
            remaining=0,
            stopped_by=nothing,
        )
    end

    log_event(log, :stage_start; stage=stage, total=length(keys), todo=length(todo))
    collisions0 = master.collisions

    # Dispatch strategy: fan out when Distributed workers are present (unless the
    # caller forced `workers=:sequential`), otherwise draw the queue on this process.
    t_prepare = time()
    mods = vcat([:ParamIO, :DataVault, :SweepRunner], _worker_module_names(load))
    if multi
        # Ensure the seam packages (+ the user's work module(s) via `load=`) are loaded in `Main`
        # on every worker before fan-out. `init_workers!` spawns workers with `--project` but loads
        # no packages, so the first dispatched key would otherwise die with a cryptic
        # `KeyError: <Module> not found` (DataKey deserialization / the save! pipeline / work_fn).
        # Idempotent, so it composes with a project that still broadcasts modules by hand.
        _ensure_worker_modules(mods)
    end
    # The same two steps for workers that join after the round began. One that cannot be readied
    # is not handed work, and the round goes on without it.
    prepare =
        pids -> try
            _ensure_worker_modules(mods)
            _observe_late!(vault, pids, observe, log, stage)
            _redirect_late!(pids)
            pids
        catch e
            e isa InterruptException && rethrow()
            log_event(
                log,
                :workers_rejected;
                level=:warn,
                stage=stage,
                n=length(pids),
                err=_short_err(e),
            )
            Int[]
        end
    # Every process that will write markers observes its sources now, so each `.done` names the
    # observation of the process that computed it (see Observe.jl).
    _observe_processes!(vault, multi, observe, log, stage)
    # The master's view of the round: one pass over the markers, then the queue the dispatcher
    # draws from. The sequential path visits keys in the caller's order, so it takes no affinity.
    prepare_secs = time() - t_prepare
    # A hook the caller gave is taken at its word: a key it has no answer for is not assumed to
    # fit a deadline. A table this package loaded by itself is not: a class it has not seen has
    # to run once to be seen.
    strict_cost = cost !== nothing || min_time !== nothing
    cost = _default_cost(vault, cost, key_class, todo, log, stage)
    _say_ignored(log, stage, opts, cost; pool=pool, affinity=affinity, spawn=spawn)
    todo = _ordered(todo, opts, cost)
    t_scan = time()
    # Whether a key can still get somewhere before the deadline; asked at each hand-out.
    # Without a `min_time`, the whole key (`cost`) — except for a key that has shown it keeps
    # progress: that one needs only the time to its next checkpoint to get somewhere.
    table_ref = Ref{Union{TaskTable,Nothing}}(nothing)
    need_time = if min_time !== nothing
        min_time
    else
        _checkpoint_aware(something(cost, Returns(0.0)), opts, table_ref)
    end
    if pool !== nothing
        # At the size the pool will run the key with: a key `:finish_by` gave more cores to is
        # not then refused for the time it would have taken on fewer.
        base_time = need_time
        need_time = key -> _pool_min_time(pool, key, opts.deadline, base_time)
    end
    unknown = Ref(0)
    fits = _fits(opts, need_time; strict=strict_cost, unknown=unknown)
    table = TaskTable(todo; affinity=multi ? affinity : nothing)
    table_ref[] = table
    scan = _scan!(table, vault, stage, log, opts)
    scan_secs = time() - t_scan
    master.locks = Dict{String,Any}(String(k) => v for (k, v) in pairs(scan))
    _apply_standing!(master, table)
    # Merge what has finished so far into the manifest, at most every `manifest_interval`.
    flushed = Ref(time())
    manifest_secs = Ref(0.0)
    flush_manifest =
        () -> begin
            (opts.manifest_interval > 0 && time() - flushed[] >= opts.manifest_interval) || return nothing
            t_flush = time()
            flushed[] = t_flush
            try
                done_now = lock(table.lock) do
                    return DataKey[
                        r.key for r in table.rows if
                        r.outcome === :ok || r.outcome === :already_done
                    ]
                end
                foreach(k -> add_complete!(m, k), done_now)
                merge_and_save_manifest!(m)
                # The cost table with it: a job killed at its wall clock leaves what it measured.
                write_cost_table(vault)
            catch e
                e isa InterruptException && rethrow()
                log_event(
                    log, :manifest_failed; level=:warn, stage=stage, err=_short_err(e)
                )
            end
            manifest_secs[] += time() - t_flush
            return nothing
        end
    master.table = table
    master.state = :running
    _identify_workers!(master, multi ? workers() : [myid()])
    drive = if multi
        () -> _drive_workers!(
            work_fn,
            vault,
            table,
            stage,
            log,
            opts,
            master;
            affinity=affinity,
            prepare=prepare,
            key_class=key_class,
            fits=fits,
            tick=flush_manifest,
            pool=pool,
            min_time=something(min_time, cost, Some(nothing)),
        )
    else
        () -> _drive_sequential!(
            work_fn,
            vault,
            table,
            stage,
            log,
            opts,
            master;
            key_class=key_class,
            fits=fits,
            tick=flush_manifest,
        )
    end
    t_dispatch = time()
    try
        _with_status(master, log) do
            return _dispatch!(
                drive,
                table,
                vault,
                stage,
                log,
                opts;
                standing=() -> _apply_standing!(master, table),
            )
        end
    finally
        # A master that is leaving says so. On the way out through an exception (an interrupt, a
        # failed worker bootstrap) there can be keys still out: the locks this master named for
        # them are released now, and each key that was cut is logged, rather than left on disk
        # for whoever trips over them.
        _release_running!(table, vault, stage, log)
        # A status that still says `running` after the master has left is the one thing it must
        # not say.
        master.state = after
        master.interval > 0 && status_tick!(master, log)
        # A master of its own ends here; one handed in (`run_loop!`) reports when the loop ends.
        own && log_event(log, :job_account; stage=stage, account=account_snapshot(master))
        # A pool's workers go with the call that was given the pool (a `run_loop!` removes them
        # when IT returns).
        (own && pool !== nothing && !pool.keep) && shutdown!(pool)
    end

    # Aggregate outcomes into counters + manifest updates.
    n_done = 0
    n_err = 0
    n_busy = 0
    n_gave_up = 0
    n_stop = 0
    n_cancelled = 0
    n_held_back = 0
    n_complete = 0
    stop_seen = nothing
    for row in table.rows
        key, outcome = row.key, row.outcome
        if outcome === :already_done
            add_complete!(m, key)
            n_complete += 1
        elseif outcome === :ok
            add_complete!(m, key)
            n_done += 1
            n_complete += 1
        elseif outcome === :stop_flag
            n_stop += 1
            stop_seen = :flag                 # outranks :deadline, as `_stop_reason` does
        elseif outcome === :stop_deadline
            n_stop += 1
            stop_seen === :flag || (stop_seen = :deadline)
        elseif outcome === :stop_request || outcome === :stopped
            # A unit that left at a safe point, or was cut: stopped, whoever asked. The job's own
            # bounds outrank a request when both hold.
            n_stop += 1
            stop_seen === nothing &&
                (stop_seen = something(_stop_reason(opts), master.ctl.stop_why))
        elseif outcome === :cancelled
            n_cancelled += 1
        elseif outcome === :no_fit
            n_held_back += 1
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
    t_manifest = time()
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
        cancelled=n_cancelled,
        held_back=n_held_back,
        collisions=master.collisions - collisions0,
        skipped=length(keys) - length(todo),
        stopped_by=stopped_by === nothing ? nothing : String(stopped_by),
        # Where the round's wall time went, outside the keys themselves.
        prepare_secs=round(prepare_secs; digits=3),
        scan_secs=round(scan_secs; digits=3),
        manifest_secs=round(manifest_secs[] + (time() - t_manifest); digits=3),
        dispatch_secs=round(t_manifest - t_dispatch; digits=3),
        total_secs=round(time() - t_run; digits=3),
    )
    # Said once, with the count: the keys this job did not start because they could not get
    # anywhere before its deadline.
    n_held_back > 0 && log_event(
        log,
        :held_back;
        stage=stage,
        keys=n_held_back,
        cost_unknown=unknown[],
        secs_left=if opts.deadline === nothing
            nothing
        else
            round(Int, opts.deadline - time())
        end,
    )
    return (
        stage=stage,
        done=n_done,
        err=n_err,
        busy=n_busy,
        gave_up=n_gave_up,
        stop=n_stop,
        cancelled=n_cancelled,
        held_back=n_held_back,
        collisions=master.collisions - collisions0,
        skipped=length(keys) - length(todo),
        # What the manifest already had, plus every row of the table: the keys a request added
        # while the round ran are rows too.
        total=(length(keys) - length(todo)) + length(table),
        # Keys of this sweep that are not done after the round, whoever would do them.
        remaining=length(table) - n_complete,
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
    watch::StopWatch=StopWatch(time(), "", ""),
    class::AbstractString="",
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
        _run_one_with_retry!(
            work_fn,
            vault,
            key,
            kstr,
            stage,
            log,
            opts,
            tok;
            resume=resume,
            watch=watch,
            class=class,
        )
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
#   :reaped — a lock whose holder is positively gone was removed; queue it
#   :stale  — nobody answered for it and it is past `stale_after`; queue it, `acquire_running!`
#             reclaims
#   :held   — held, or not decidable yet; do not queue it this pass
#
# `masters` is a thunk: the masters' status files are read only if a lock turns up, and once.
function _scan_row!(
    table::TaskTable,
    i::Int,
    vault::Vault,
    stage::Symbol,
    log::EventLog,
    opts::RunOpts,
    progress::Dict{String,Progress},
    masters,
    infos::Vector{LockInfo},
)::Symbol
    r = table.rows[i]
    if DataVault.is_done(vault, r.key)
        settle!(table, i, :already_done)
        return :done
    end
    r.progress = get(progress, r.kstr, nothing)
    lk = _lock_now(vault, r.key)
    lk === nothing && return :free
    owner = lk.owner
    info = _lock_info(r.kstr, owner, lk.age, masters(), _NO_PAGES, opts.stale_after)
    push!(infos, info)
    if info.verdict === :dead
        # Owner-checked: `false` means the lock changed hands since it was judged, and whoever
        # has it now is asked about on the next pass.
        _reap!(vault, r.key, info, stage, log) && return :reaped
    elseif info.verdict === :stale
        return :stale
    end
    hold!(table, i, owner)
    log_event(log, :lock_busy; level=:debug, stage=stage, key=r.kstr)
    return :held
end

const _NO_PAGES = Dict{String,Float64}()

# The masters' statuses, read at most once per scan and only if a lock is found.
function _lazy_masters(vault::Vault)
    cache = Ref{Any}(nothing)
    return () -> begin
        cache[] === nothing && (cache[] = read_status(vault))
        return cache[]
    end
end

"""
    _scan!(table, vault, stage, log, opts) -> NamedTuple

The master's one pass over the markers, before the queue is drawn: every queued row is checked for
a `.done` written since the manifest and for a `.running` lock, and the recorded progress is
attached. Each lock found is judged ([`judge_lock`](@ref)): one that is positively dead is removed
and its key queued, one that is held is kept out of the queue. Returns
`(; done, locks, held, held_jobs, reaped, dead_jobs, stale, unknown)`, and logs it as
`locks_reconciled` when there was any lock.

This is the read the workers used to do one key at a time, and the reconciliation a job that
starts used to skip: it joined a vault without knowing whose locks were in it.
"""
function _scan!(table::TaskTable, vault::Vault, stage::Symbol, log::EventLog, opts::RunOpts)
    lost = Ref(0)
    progress = read_progress(vault; unreadable=lost)
    # Those keys start without their resume point: said, with how many.
    lost[] > 0 &&
        log_event(log, :progress_unreadable; level=:warn, stage=stage, files=lost[])
    masters = _lazy_masters(vault)
    infos = LockInfo[]
    done = reaped = 0
    for i in eachindex(table.rows)
        table.rows[i].state === :todo || continue
        s = _scan_row!(table, i, vault, stage, log, opts, progress, masters, infos)
        s === :done && (done += 1)
        s === :reaped && (reaped += 1)
    end
    sm = lock_summary(infos)
    out = (;
        done=done,
        locks=length(infos),
        held=sm.held,
        held_jobs=sm.held_jobs,
        reaped=reaped,
        dead_jobs=sm.dead_jobs,
        stale=sm.stale,
        unknown=sm.unknown,
    )
    isempty(infos) || log_event(log, :locks_reconciled; stage=stage, out...)
    return out
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
    masters = _lazy_masters(vault)
    infos = LockInfo[]
    n = 0
    for i in busy
        s = _scan_row!(table, i, vault, stage, log, opts, progress, masters, infos)
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
    drive,
    table::TaskTable,
    vault::Vault,
    stage::Symbol,
    log::EventLog,
    opts::RunOpts;
    standing=Returns(nothing),
)
    round = 0
    while true
        before = _n_finished(table)
        for _ in 0:_BUSY_RESCANS
            drive()
            _stop_reason(opts) === nothing || break
            _rescan_busy!(table, vault, stage, log, opts) == 0 && break
            # What was just put back is still subject to what requests cancelled or moved.
            standing()
        end
        deferred = [i for (i, r) in enumerate(table.rows) if r.outcome === :deferred]
        isempty(deferred) && break
        _stop_reason(opts) === nothing || break
        _n_finished(table) > before || sleep(opts.defer_poll)
        round += 1
        log_event(log, :deferred_round; stage=stage, round=round, keys=length(deferred))
        foreach(i -> requeue!(table, i), deferred)
        standing()
    end
    return nothing
end

"""
    _drive_sequential!(work_fn, vault, table, stage, log, opts, master)

Draw the queue on this process, one key at a time. Between keys the status is refreshed and the
control requests are applied: no timer fires while this process is inside `work_fn`.
"""
function _drive_sequential!(
    work_fn::Function,
    vault::Vault,
    table::TaskTable,
    stage::Symbol,
    log::EventLog,
    opts::RunOpts,
    master::Master;
    key_class=nothing,
    fits=Returns(true),
    tick=Returns(nothing),
)
    c = master.ctl
    while true
        _status_due!(master, log)
        tick()
        poll_control!(master, table, log, opts)
        # The keys a stop drops are ATTRIBUTED, not silently absent: every row ends the round
        # with an outcome.
        stop = _stop_reason(opts, master)
        if stop !== nothing
            settle_queued!(table, _stop_outcome(stop))
            break
        end
        if c.paused
            # Nothing is handed out while paused, and the queue is kept: a request ends it.
            sleep(min(max(opts.control_interval, 0.05), 1.0))
            poll_control!(master, table, log, opts; force=true)
            continue
        end
        i = _next_fitting!(table, myid(), fits)
        i === nothing && break
        row = table.rows[i]
        tok = owner_token()
        start_task!(table, i, tok, myid())
        _out_add!(tok, vault, row.key)
        t0 = time()
        outcome = try
            last(
                _run_one_with_lock!(
                    work_fn,
                    vault,
                    row.key,
                    stage,
                    log,
                    opts;
                    tok=tok,
                    resume=row.progress,
                    reap=false,
                    watch=StopWatch(master.started, master.id, master.job),
                    class=_class_of(key_class, row.key),
                ),
            )
        finally
            _out_remove!(tok)
        end
        delete!(c.stopping, row.kstr)
        outcome === :lock_busy && (master.collisions += 1)
        _account_key!(master, vault, row, myid(), t0, outcome)
        settle!(table, i, outcome)
    end
    return nothing
end

# Book the time a key was out. A key that did not finish keeps what it had reported as progress
# during this attempt; the rest of the attempt is lost.
function _account_key!(
    master::Master, vault::Vault, row::TaskRow, worker::Int, t0::Float64, outcome
)
    t1 = time()
    finished = outcome === :ok || outcome === :already_done
    at = nothing
    if !finished
        p = _read_progress_one(vault, row.kstr)
        (p !== nothing && p.at >= t0) && (at = p.at)
    end
    # Work was thrown away, as opposed to the key coming straight back because someone held it.
    cut = outcome === nothing || outcome in (:error, :gave_up, :stopped)
    _acct_key!(master.acct, worker, t0, t1, finished, at; cut=cut)
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

# Release the lock of every row that is still out, and say which keys were cut. A normal round
# has none: this is for a master leaving through an exception.
function _release_running!(table::TaskTable, vault::Vault, stage::Symbol, log::EventLog)
    cut = [(i, r) for (i, r) in enumerate(table.rows) if r.state === :running]
    for (i, r) in cut
        tok = r.owner
        tok === nothing && continue
        try
            DataVault.clear_running!(vault, r.key, tok)
            log_event(
                log, :lock_released; stage=stage, key=r.kstr, owner=tok, why="master_exit"
            )
        catch e
            e isa InterruptException && rethrow()
            # Said as what it is: the lock is still there, until `stale_after`.
            log_event(
                log,
                :release_failed;
                level=:warn,
                stage=stage,
                key=r.kstr,
                owner=tok,
                err=_short_err(e),
            )
        end
        _out_remove!(tok)
        settle!(table, i, :lock_busy)
    end
    return length(cut)
end

"""
    _drive_workers!(work_fn, vault, table, stage, log, opts, master; affinity, prepare)

Draw the queue over the Distributed workers: one dispatch task per worker, each taking the next
row [`next_task!`](@ref) gives it, handing the worker the key WITH its lock token and resume
point, and settling the row with what comes back.

The master names the lock (`owner_token(host, pid)` of the worker), so the table knows who holds
what while it runs, and a worker that dies has its lock released at once.

A dispatch task does not leave while a key is still out: a worker that dies gives its key back,
and somebody has to be there to take it. A key that has taken down
`_WORKER_DEATH_REDISPATCHES + 1` workers is reported rather than handed to the next one —
unbounded, a key that reliably kills whoever takes it is handed to worker after worker forever.

A ticker runs beside the dispatch tasks, every `opts.control_interval` seconds. It applies control
requests ([`poll_control!`](@ref)) and adopts workers that joined since the round began: a pool
that is still ramping up, or one grown by a `:resize`, is used as it arrives rather than from the
next round. `prepare(pids)` readies late workers (modules, source observation) and returns the
ones that can be handed work.
"""
function _drive_workers!(
    work_fn::Function,
    vault::Vault,
    table::TaskTable,
    stage::Symbol,
    log::EventLog,
    opts::RunOpts,
    master::Master;
    affinity=nothing,
    prepare=nothing,
    key_class=nothing,
    fits=Returns(true),
    tick=Returns(nothing),
    pool=nothing,
    min_time=nothing,
)
    c = master.ctl
    # All dispatch tasks and the ticker are `@async` on this task's thread, so a plain counter and
    # Condition are enough: nothing between a check and the `wait` that follows it can yield.
    out = Ref(0)
    idle = Condition()
    stopped = Ref(false)
    rid = rand(UInt64)
    unidentified = Set{Int}()
    graced = Ref(false)
    # With a pool: whether anything is queued, kept by the ticker, so a dispatch task whose size
    # fits nothing right now waits for a key that will come back rather than leaving.
    queued_now = Ref(pool !== nothing)
    # Whether keys are being held back by other masters' locks: why an idle worker is idle.
    held_now = Ref(any(r -> r.state === :held, table.rows))
    started = Set{Int}()
    tasks = Task[]

    function _loop(pid::Int, host::String, ospid::Int)
        # The round's context goes to the worker once. Sent with every key, the work function,
        # the vault (its whole spec), the log and the options were serialised again per
        # dispatch, through the master's one core.
        try
            remotecall_fetch(_install_round, pid, rid, work_fn, vault, stage, log, opts)
        catch e
            e isa ProcessExitedException && return nothing
            log_event(
                log, :workers_rejected; level=:warn, stage=stage, n=1, err=_short_err(e)
            )
            return nothing
        end
        while true
            if !stopped[]
                stop = _stop_reason(opts, master)
                if stop !== nothing
                    settle_queued!(table, _stop_outcome(stop))
                    stopped[] = true
                end
            end
            # A worker that went away while this loop was waiting must not be handed a key: the
            # call would fail at once and be counted against the key as a death.
            pid in workers() || break
            if pid in c.retired
                # A `:resize` took this worker out. It leaves the pool, which is what frees its
                # cores; it was between units, so nothing is cut.
                log_event(log, :worker_retired; stage=stage, worker=pid, host=host)
                @async try
                    rmprocs(pid; waitfor=30)
                catch
                end
                break
            end
            host in c.drained && break
            # The pool retires its own workers; the task only has to leave.
            (pool !== nothing && _pool_retiring(pool, pid)) && break
            accept = if pool === nothing
                nothing
            else
                row -> _pool_accepts(pool, pid, row, opts.deadline, min_time)
            end
            i = if c.paused || stopped[]
                nothing
            else
                _next_fitting!(table, pid, fits; accept=accept)
            end
            if i === nothing
                # Leave when nothing is out and nothing can arrive. A paused master keeps its
                # queue, and its dispatch tasks with it; so does a pool with keys still queued
                # that this worker's size does not fit yet.
                out[] == 0 &&
                    !(c.paused && !stopped[] && _has_queued(table)) &&
                    !(queued_now[] && !stopped[]) &&
                    break
                w0 = time()
                why = if c.paused
                    :paused
                elseif stopped[]
                    :stopping
                elseif held_now[]
                    :lock_busy
                else
                    :queue_empty
                end
                wait(idle)
                _acct_idle!(master.acct, pid, why, w0, time())
                continue
            end
            row = table.rows[i]
            tok = owner_token(host, ospid)
            start_task!(table, i, tok, pid)
            _out_add!(tok, vault, row.key)
            out[] += 1
            t0 = time()
            died = false
            outcome = try
                last(
                    remotecall_fetch(
                        _run_installed,
                        pid,
                        rid,
                        row.key;
                        tok=tok,
                        resume=row.progress,
                        reap=false,
                        watch=StopWatch(master.started, master.id, master.job),
                        class=_class_of(key_class, row.key),
                    ),
                )
            catch e
                if e isa ProcessExitedException
                    died = true
                    _release_dead!(vault, row, tok, stage, log)
                    # The worker cannot say what the attempt cost; the master knows how long.
                    log_event(
                        log,
                        :key_spent;
                        stage=stage,
                        key=row.kstr,
                        outcome="worker_died",
                        secs=time() - t0,
                        cores=get(master.acct.cores, pid, 0),
                        host=host,
                        class=_class_of(key_class, row.key),
                    )
                    ord = get(c.stopping, row.kstr, nothing)
                    if ord !== nothing && ord.cut
                        # Removed on purpose: not a death of the key, and not its memory.
                        :stopped
                    else
                        pool === nothing || _pool_death!(pool, row, pid, log, stage)
                        row.deaths += 1
                        _after_death(row, log, stage)
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
                _out_remove!(tok)
            end
            order = get(c.stopping, row.kstr, nothing)
            if order !== nothing
                delete!(c.stopping, row.kstr)
                # A unit that was cut comes back as a lost lock, an error from the interrupt, or
                # not at all. Whatever it is, the unit was stopped, not failed.
                order.cut && outcome !== :ok && (outcome = :stopped)
                # Told to stop, and its worker died before it left: it stays stopped. Requeued,
                # it would be the next key handed out.
                outcome === nothing && (outcome = :stopped)
            end
            # The same for a key a request cancelled while it was running.
            if outcome === nothing && any(f -> matches(f, row.key), c.cancels)
                outcome = :cancelled
            end
            outcome === :lock_busy && (master.collisions += 1)
            _account_key!(master, vault, row, pid, t0, outcome)
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

    # Start a dispatch task for every worker that does not have one. Workers present when the
    # round began were prepared by `run!`; later ones are prepared here.
    function _adopt!()
        # `workers()` is `[1]` when there are none: the master is not one of its own workers.
        fresh = [p for p in workers() if !(p in started) && p != myid()]
        # A worker the pool is still starting is visible here before the pool knows its size;
        # it gets a dispatch task once it is registered, not before.
        pool === nothing || filter!(p -> _pool_adoptable(pool, p), fresh)
        isempty(fresh) && return nothing
        late = !isempty(started)
        # Workers `run!` found were readied by it; a pool's own, and any that join later, here.
        mine = pool === nothing ? Int[] : [p for p in fresh if !(p in pool.foreign)]
        ready = if prepare === nothing
            fresh
        elseif late
            prepare(fresh)
        else
            vcat(setdiff(fresh, mine), isempty(mine) ? Int[] : prepare(mine))
        end
        _identify_workers!(master, ready)
        who = lock(() -> copy(master.who), master.lock)
        n = 0
        for pid in ready
            w = get(who, pid, nothing)
            if w === nothing
                # Could not say who it is: asked again next tick, and said once.
                if !(pid in unidentified)
                    push!(unidentified, pid)
                    log_event(
                        log,
                        :workers_rejected;
                        level=:warn,
                        stage=stage,
                        n=1,
                        err="worker $pid did not answer who it is; it gets no work until it does",
                    )
                end
                continue
            end
            push!(started, pid)
            n += 1
            push!(tasks, @async try
                _loop(pid, w.host, w.pid)
            finally
                # A loop that leaves on an exception must not strand the ones waiting on it.
                notify(idle)
            end)
        end
        late && log_event(log, :workers_joined; stage=stage, n=n)
        return nothing
    end

    pool === nothing ||
        _pool_tick!(pool, table, master, log, stage, opts, min_time; fits=fits)
    _adopt!()
    done = Ref(false)
    idle_since = Ref(0.0)
    # `every`, not `tick`: `tick` is the caller's per-tick callback (a keyword of this function).
    every = opts.control_interval > 0 ? opts.control_interval : 10.0
    # A pool is looked at more often than requests are: a start it does not make is idle room.
    pool === nothing || (every = min(every, pool.poll))
    @async while true
        sleep(every)
        done[] && break
        try
            poll_control!(
                master, table, log, opts; affinity=affinity, force=pool === nothing
            )
            if pool !== nothing
                _pool_tick!(pool, table, master, log, stage, opts, min_time; fits=fits)
                queued_now[] = _pool_wants(pool, table)
            end
            _adopt!()
            _leave_if_underused!(master, table, out[], idle_since, opts, log)
            # The job's own stop gets its grace once, when it is first seen.
            if !graced[] && _stop_reason(opts) !== nothing
                graced[] = true
                _order_stops_all!(master, table, opts.stop_grace, "stop_grace")
            end
            _enforce_stops!(master, table, log)
            tick()
            held_now[] = lock(() -> any(r -> r.state === :held, table.rows), table.lock)
        catch e
            log_event(log, :control_failed; level=:warn, stage=stage, err=_short_err(e))
        end
        # Wakes the idle dispatch tasks: a resume, a new key or a new stop is theirs to act on.
        notify(idle)
    end

    failure = nothing
    try
        i = 1
        while true
            while i <= length(tasks)               # the list grows as workers are adopted
                try
                    wait(tasks[i])
                catch e
                    failure === nothing && (failure = e)
                end
                i += 1
            end
            # A pool may have nothing started yet, or be starting the workers the queue still
            # needs: the round is not over while it is working towards them.
            (
                pool !== nothing &&
                failure === nothing &&
                _stop_reason(opts, master) === nothing
            ) || break
            _pool_wants(pool, table) || break
            sleep(min(every, 0.2))
        end
    finally
        done[] = true
        # The workers can forget this round.
        for pid in started
            pid in workers() && remote_do(_drop_round, pid, rid)
        end
    end
    # A cut removes a worker and then releases its lock; the round is over when that is done,
    # so what it reports (and what the next round finds on disk) is settled.
    for t in c.cuts
        try
            wait(t)
        catch
        end
    end
    empty!(c.cuts)
    failure === nothing || throw(failure)
    # A pool that cannot start workers is not a round that ended: said, and an error.
    if pool !== nothing && _pool_gave_up(pool) && _has_queued(table)
        log_event(
            log,
            :pool_gave_up;
            level=:error,
            stage=stage,
            fails=pool.fails,
            queued=count(r -> r.state === :todo, table.rows),
        )
        error(
            "SizedPool: $(pool.fails) worker starts failed in a row with keys still queued. " *
            "The reasons are in the event log (kind=\"pool_spawn_failed\" / " *
            "\"pool_spawn_short\").",
        )
    end

    # Keys still queued with nobody left to take them. Under a stop they are attributed to it;
    # otherwise every worker died (or was drained or retired) while they were pending: they were
    # never attempted, so they are retriable rather than failed, and `run!` counts them with
    # `busy`.
    stop = _stop_reason(opts, master)
    if stop !== nothing
        settle_queued!(table, _stop_outcome(stop))
    else
        for (i, r) in enumerate(table.rows)
            r.state === :todo || continue
            log_event(log, :worker_lost; stage=stage, key=r.kstr)
            settle!(table, i, :worker_lost)
        end
    end
    return nothing
end

# What becomes of a key whose worker died under it: back on the queue (`nothing`), or, once it
# has taken down `_WORKER_DEATH_REDISPATCHES + 1` workers, given up on.
function _after_death(row::TaskRow, log::EventLog, stage::Symbol)
    row.deaths > _WORKER_DEATH_REDISPATCHES || return nothing
    log_event(
        log,
        :gave_up;
        stage=stage,
        key=row.kstr,
        attempts=row.deaths,
        err="worker exited on this key every time it was dispatched",
    )
    # The outcome the event names: counted in `gave_up` (and in `err`).
    return :gave_up
end

# The queue is empty and most of the workers have nothing: the job is holding its nodes for a few
# long units. After `opts.idle_grace` of that, stop on purpose. The request goes through the
# control channel so the units still running see it at their next `should_stop`.
function _leave_if_underused!(
    m::Master,
    table::TaskTable,
    busy::Int,
    since::Base.RefValue{Float64},
    opts::RunOpts,
    log::EventLog,
)
    (opts.min_busy_fraction > 0 && !m.ctl.stop_all) || return false
    who = lock(() -> copy(m.who), m.lock)
    mine = [
        p for p in workers() if
        !(p in m.ctl.retired) && !(haskey(who, p) && who[p].host in m.ctl.drained)
    ]
    n = length(mine)
    # In cores, not workers: with workers of different sizes (a pool), one busy 32-core worker
    # among idle 1-core ones is a job that is mostly in use.
    cores(p) = haskey(who, p) ? max(who[p].cores, 1) : 1
    on = lock(table.lock) do
        return Set(r.worker for r in table.rows if r.state === :running && r.worker != 0)
    end
    total = sum(cores, mine; init=0)
    used = sum(cores, (p for p in mine if p in on); init=0)
    if n == 0 || _has_queued(table) || used / total >= opts.min_busy_fraction
        since[] = 0.0
        return false
    end
    since[] == 0.0 && (since[] = time())
    lasted = time() - since[]
    lasted >= opts.idle_grace || return false
    m.ctl.stop_all = true
    m.ctl.stop_why = :underused
    log_event(
        log,
        :underused;
        level=:warn,
        stage=m.stage,
        busy=busy,
        workers=n,
        cores_busy=used,
        cores=total,
        secs=round(Int, lasted),
    )
    try
        # Written whatever `control_interval` is: this is how the running units hear of it
        # (`should_stop` reads the requests itself).
        control!(m.vault, :stop; master=m.id)
    catch e
        e isa InterruptException && rethrow()
    end
    return true
end

function _has_queued(table::TaskTable)
    return lock(() -> any(r -> r.state === :todo, table.rows), table.lock)
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
    watch::StopWatch=StopWatch(time(), "", ""),
    class::AbstractString="",
)
    last_err = nothing
    reported = Ref(resume !== nothing)
    for attempt in 1:opts.max_attempts
        log_event(log, :key_start; level=:debug, stage=stage, key=kstr, attempt=attempt)
        t0 = time()
        cpu0 = _cpu_seconds()
        notes = Dict{String,Any}()
        # The peak is counted from here, so it is this key's and not the worker's lifetime's —
        # where that reset works; where it does not, the record says the peak is the process's.
        scope = _reset_peak_rss() ? "key" : "process"
        # What this attempt cost, for an attempt that did not finish the key: without it the
        # classes that fail, are cut, or run long and resume are the ones with no data.
        spent =
            outcome -> log_event(
                log,
                :key_spent;
                stage=stage,
                key=kstr,
                outcome=String(outcome),
                secs=time() - t0,
                attempt=attempt,
                cpu=_finite(_cpu_seconds() - cpu0),
                cores=_my_cores(),
                rss=_peak_rss(),
                rss_scope=scope,
                host=gethostname(),
                class=String(class),
            )
        try
            # What `work_fn` can ask about the key it was handed (`resume_point`,
            # `report_progress`). A retry starts from what the failed attempt reported.
            if attempt > 1 && reported[]
                resume = _read_progress_one(vault, kstr)
            end
            ctx = KeyContext(
                vault,
                key,
                kstr,
                String(tok),
                resume,
                opts,
                reported,
                watch,
                notes,
                CheckpointState(),
                log,
                stage,
            )
            payload = with(() -> work_fn(key), _KEY => ctx)
            payload isa Dict || error(
                "work_fn must return a Dict (got $(typeof(payload))). " *
                "Wrap scalars as e.g. Dict(\"value\" => x).",
            )
            if DataVault.running_owner(vault, key) != tok
                # A sibling master reclaimed our lock while work_fn ran; it now
                # owns this key. Discard our result rather than double-committing.
                log_event(log, :lock_lost; stage=stage, key=kstr, attempt=attempt)
                spent(:lock_lost)
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
                spent(:lock_lost)
                return :lock_busy
            end
            # A finished unit has no resume point. Only when one was written, so a unit that
            # never reports costs no extra filesystem call.
            if reported[]
                _clear_progress(vault, kstr)
                _clear_checkpoint(vault, kstr)
            end
            log_event(
                log,
                :key_done;
                stage=stage,
                key=kstr,
                secs=time() - t0,
                attempt=attempt,
                sha256=saved.sha256,
                # What the key cost (Cost.jl reads these back): CPU seconds over all threads,
                # the cores this worker has, peak resident bytes, where it ran.
                cpu=_finite(_cpu_seconds() - cpu0),
                cores=_my_cores(),
                rss=_peak_rss(),
                rss_scope=scope,
                host=gethostname(),
                class=String(class),
                note=notes,
            )
            return :ok
        catch e
            # Not a failure: the artifact this key needs is being built elsewhere. Hand the key
            # back without spending an attempt; `run!` re-dispatches it once the pass drains.
            if e isa DataVault.ArtifactBusy
                log_event(log, :artifact_busy; stage=stage, key=kstr, artifact=e.name)
                return :deferred
            end
            # Not a failure either: the unit was told to stop and left at a safe point.
            if e isa StopRequested
                log_event(log, :key_stopped; stage=stage, key=kstr, attempt=attempt)
                spent(:stopped)
                return :stopped
            end
            # A unit whose lock is gone (cut after its grace, or reclaimed) has nothing to retry
            # for: its result would be refused at the commit.
            # The error is on record first: a unit that failed AND lost its lock used to leave only
            # `lock_lost`, and the exception was never seen.
            last_err = _short_err(e)
            log_event(log, :error; stage=stage, key=kstr, attempt=attempt, err=last_err)
            spent(:error)
            if DataVault.running_owner(vault, key) != tok
                log_event(log, :lock_lost; stage=stage, key=kstr, attempt=attempt)
                return :lock_busy
            end
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
    run_loop!(work_fn, vault, keys; opts=RunOpts(), max_empty_rounds=3, idle_sleep=30.0,
              load=nothing, prerequisite=nothing, affinity=nothing, observe=true,
              spawn=nothing, key_class=nothing, cost=nothing, min_time=nothing,
              pool=nothing) -> NamedTuple

Work-stealing loop that repeatedly calls [`run!`](@ref) until there is no
more work to do. This is the infra equivalent of FiniteTemperature.jl's
`_work_loop` driver.

The loop exits when:
- a round leaves no key undone (`remaining == 0`), at once, or
- `max_empty_rounds` consecutive rounds produce zero new completions AND leave nothing held by a
  sibling, or
- `opts.stop_flag` is raised, or `opts.deadline` has passed, or
- a round held keys back because they could not get anywhere before `opts.deadline`
  (`held_back > 0`): the loop returns `stopped_by = :deadline` instead of sitting out idle rounds
  over keys it will not start, or
- a round ended `:underused` (`opts.min_busy_fraction`) or on a `:stop` request with no scope, or
- keys are still held by a sibling after the busy budget below: the loop returns with
  `busy > 0`, `remaining > 0`.

A round that completes nothing but finds keys `:lock_busy` does NOT count toward
`max_empty_rounds` until `opts.stale_after + 2 * idle_sleep` seconds of such rounds have passed
(the busy budget). Those keys are either being worked on by a live sibling, or held by one the
wall clock killed. A holder that can be shown to be gone is reaped at the start of the next
round; `stale_after` separates the two only where nobody can be asked, and past it
`acquire_running!` reclaims the lock. Returning before then leaves the campaign short and
reports nothing, because `max_empty_rounds * idle_sleep` (90 s by default) is an order of
magnitude under `stale_after` (600 s).

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

`affinity`, `spawn`, `key_class`, `cost`, `min_time` and `pool` are forwarded verbatim to every [`run!`](@ref) call. The loop is one
[`Master`](@ref) for all its rounds, so what a [`control!`](@ref) request changed — a cancelled
filter, enqueued keys, a pause — holds from round to round, and a `:stop` with no scope ends the
loop (`stopped_by = :request`).

Returns `(; ran, rounds, done, busy, err, gave_up, remaining, collisions, stopped_by,
prerequisite)`. `collisions` is the total over the rounds (keys handed out that another master
had taken: what sharding is there to lower). `busy`
is how many keys the last round found held by a sibling, so a caller can tell "everything is
done" from "someone else still has work out". `err`, `gave_up` and `remaining` are the last
round's: a stage whose remaining keys all fail is `done = 0, busy = 0` like a clean finish, and
`err > 0`, `remaining > 0` is what tells them apart. `ran` is `false` exactly when a prerequisite
blocked the stage.
"""
function run_loop!(
    work_fn::Function, vault::Vault, keys::AbstractVector{DataKey}; pool=nothing, kwargs...
)
    try
        return _run_loop!(work_fn, vault, keys; pool=pool, kwargs...)
    finally
        # Whatever way the loop ended, the pool's workers do not outlive it.
        (pool !== nothing && !pool.keep) && shutdown!(pool)
    end
end

function _run_loop!(
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
    spawn=nothing,
    key_class=nothing,
    cost=nothing,
    min_time=nothing,
    pool=nothing,
)
    pre = nothing
    if prerequisite !== nothing
        pre = run_prerequisite!(prerequisite; opts=opts, load=load, poll=idle_sleep)
        pre.complete || return (;
            ran=false,
            rounds=0,
            done=0,
            busy=0,
            err=0,
            gave_up=0,
            remaining=length(keys),
            collisions=0,
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
    n_err = 0
    n_gave_up = 0
    n_remaining = length(keys)
    n_collisions = 0
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
        stopped = _stop_reason(opts, master)
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
            spawn=spawn,
            key_class=key_class,
            cost=cost,
            min_time=min_time,
            pool=pool,
        )
        n_done += result.done
        n_collisions += result.collisions
        n_busy = result.busy
        n_err, n_gave_up, n_remaining = result.err, result.gave_up, result.remaining
        # Every key is done. No later round can find anything, and sitting out
        # `max_empty_rounds` idle rounds would hold the allocation for nothing.
        result.remaining == 0 && break
        # Nothing was done, nobody else holds anything, and what is left was held back: no key
        # that remains can get anywhere before the deadline. Idle rounds would not change that.
        if result.done == 0 && result.busy == 0 && result.held_back > 0
            stopped = :deadline
            break
        end
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
    log_event(
        EventLog(
            joinpath(vault.outdir, "events_$(master.id).jsonl"); min_level=opts.log_level
        ),
        :job_account;
        stage=Symbol(vault.run),
        account=account_snapshot(master),
    )
    # What the keys cost, where the next job (and whatever sizes it) can read it.
    if rounds > 0
        try
            write_cost_table(vault)
        catch e
            e isa InterruptException && rethrow()
            log_event(
                EventLog(joinpath(vault.outdir, "events_$(master.id).jsonl")),
                :cost_table_failed;
                level=:warn,
                stage=Symbol(vault.run),
                err=_short_err(e),
            )
        end
    end
    return (;
        ran=true,
        rounds=rounds,
        done=n_done,
        busy=n_busy,
        # Of the LAST round: what is still failing, and what is still not done. A stage whose
        # remaining keys all fail used to come back looking like a clean finish.
        err=n_err,
        gave_up=n_gave_up,
        remaining=n_remaining,
        collisions=n_collisions,
        stopped_by=stopped,
        prerequisite=pre,
    )
end

# `todo` in the order it is drawn: the caller's, or longest expected time first; then, for a
# master that is one of several, its own share of the keys ahead of the others'.
function _ordered(todo::Vector{DataKey}, opts::RunOpts, cost)
    out = todo
    if opts.order === :longest_first && cost !== nothing
        # Stable, so keys of equal cost keep the caller's order.
        # A key of unknown cost sorts as the shortest: it is not put ahead of the measured ones.
        out = sort(out; by=k -> -something(key_seconds(cost, k), 0.0), alg=MergeSort)
    end
    sh = opts.shard
    if sh !== nothing && sh[2] > 1
        mine = [_shard_of(canonical(k), sh[2]) == sh[1] for k in out]
        out = vcat(out[mine], out[.!mine])
    end
    return out
end

# Which of `m` shares a key falls in. From the key's SHA-1, not `hash`: every master, on any
# node and Julia version, has to agree.
function _shard_of(kstr::AbstractString, m::Int)::Int
    d = sha1(String(kstr))
    v = UInt64(0)
    for b in @view d[1:8]
        v = (v << 8) | UInt64(b)
    end
    return Int(v % UInt64(m))
end

"""
    key_seconds(f, key) -> Union{Float64,Nothing}

Ask a cost hook (`cost`, `min_time`) about one key, guarded: `nothing` when the hook throws or
answers something that is not a finite, non-negative number. This is the one way the run, the
campaign and the job controller ask, so "unknown" is the same everywhere — and is never silently
zero.
"""
function key_seconds(f, key::DataKey)::Union{Float64,Nothing}
    try
        x = Float64(f(key))
        return (isfinite(x) && x >= 0) ? x : nothing
    catch e
        e isa InterruptException && rethrow()
        return nothing
    end
end

# The default `min_time`: `whole(key)`, or `checkpoint_every` when that is shorter AND the key has
# a progress record — written by `save_checkpoint!` / `report_progress` in an earlier attempt, so
# the key is known to keep what it has done. A key with no record is not assumed to checkpoint:
# started with less than its whole time left, it would lose all of it.
function _checkpoint_aware(whole, opts::RunOpts, table_ref::Ref)
    every = opts.checkpoint_every
    every > 0 || return whole
    return key -> begin
        t = whole(key)
        table = table_ref[]
        table === nothing && return t
        i = get(table.index, canonical(key), 0)
        (i == 0 || table.rows[i].progress === nothing) && return t
        return (t isa Real && !isnan(t)) ? min(t, every) : every
    end
end

# Options that have no effect on the path this round takes are said, once per round: asked for
# and silently dropped is how a sweep runs for hours in an order nobody chose.
function _say_ignored(
    log::EventLog,
    stage::Symbol,
    opts::RunOpts,
    cost;
    pool=nothing,
    affinity=nothing,
    spawn=nothing,
)
    said =
        (option, why) -> log_event(
            log, :option_ignored; level=:warn, stage=stage, option=option, why=why
        )
    if opts.order === :longest_first && cost === nothing
        said(
            "order=:longest_first",
            "no cost to order by: pass `cost`, or `key_class` once the stage has a cost table",
        )
    end
    if opts.workers === :sequential
        why = "workers=:sequential runs the keys on the master, in order"
        pool === nothing || said("pool", why)
        affinity === nothing || said("affinity", why)
        spawn === nothing || said("spawn", why)
    end
    return nothing
end

# `key -> Bool`: can this key still get somewhere before the deadline? Always, without one.
# A key whose need is unknown is not assumed to fit when the hook is the caller's (`strict`);
# `unknown` counts those.
function _fits(opts::RunOpts, need; strict::Bool=false, unknown=Ref(0))
    d = opts.deadline
    d === nothing && return Returns(true)
    return key -> begin
        t = key_seconds(need, key)
        if t === nothing
            strict || return true
            unknown[] += 1
            return false
        end
        return time() + t <= d
    end
end

# The cost hook `run!` works with: the measured table of this stage where there is one, with the
# caller's hook for the classes it has not seen. Said in the log, with how many of this round's
# keys fell back.
function _default_cost(vault::Vault, cost, key_class, todo, log::EventLog, stage::Symbol)
    key_class === nothing && return cost
    table = try
        load_cost_table(vault; strict=true)
    catch e
        e isa InterruptException && rethrow()
        log_event(log, :cost_table_unreadable; level=:warn, stage=stage, err=_short_err(e))
        return cost
    end
    isempty(table) && return cost
    fallback = cost === nothing ? (k -> NaN) : cost
    seen = count(k -> haskey(table, _class_of(key_class, k)), todo)
    log_event(
        log,
        :cost_source;
        stage=stage,
        source="measured",
        classes=length(table),
        measured_keys=seen,
        fallback_keys=length(todo) - seen,
        fallback=cost === nothing ? "none" : "the caller's cost",
    )
    return measured_cost(table, k -> _class_of(key_class, k); fallback=fallback)
end

# The next row whose key fits; the ones passed over on the way are settled `:no_fit`.
function _next_fitting!(table::TaskTable, worker::Int, fits; accept=nothing)
    while true
        i = next_task!(table, worker; accept=accept)
        i === nothing && return nothing
        fits(table.rows[i].key) && return i
        settle!(table, i, :no_fit)
    end
end

# ── a round's context, held by the worker ───────────────────────────────────────────────────────

# round id => what every key of that round is run with. On the worker.
const _ROUNDS = Dict{UInt64,Any}()
const _ROUNDS_LOCK = ReentrantLock()

function _install_round(
    rid::UInt64, work_fn, vault::Vault, stage::Symbol, log, opts::RunOpts
)
    lock(_ROUNDS_LOCK) do
        return _ROUNDS[rid] = (; work_fn, vault, stage, log, opts)
    end
    return nothing
end

_drop_round(rid::UInt64) = (lock(() -> delete!(_ROUNDS, rid), _ROUNDS_LOCK); nothing)

# `_run_one_with_lock!` for a key of an installed round: only the key and what is particular to
# this hand-out travel.
function _run_installed(rid::UInt64, key::DataKey; kwargs...)
    r = lock(() -> get(_ROUNDS, rid, nothing), _ROUNDS_LOCK)
    r === nothing && error("round $(string(rid; base=16)) is not installed on this worker")
    return _run_one_with_lock!(r.work_fn, r.vault, key, r.stage, r.log, r.opts; kwargs...)
end

# Workers that joined after `worker_logs!` was called write to the same directory.
function _redirect_late!(pids)
    dir = _WORKER_LOG_DIR[]
    dir === nothing && return nothing
    for p in pids
        try
            remotecall_fetch(_redirect_output, p, dir)
        catch e
            e isa InterruptException && rethrow()
        end
    end
    return nothing
end

"""
    todo_count(vault, keys) -> Int

How many of `keys` are not done: the manifest first, then the markers for what it does not have.
Ask it BEFORE `init_workers!`, so a job that has nothing to do leaves in seconds instead of
starting hundreds of workers to find that out:

```julia
SweepRunner.todo_count(vault, keys) == 0 && exit(0)
SweepRunner.init_workers!()
```
"""
function todo_count(vault::Vault, keys::AbstractVector{DataKey})
    todo = todo_keys(load_manifest(vault), collect(keys))
    return count(k -> !DataVault.is_done(vault, k), todo)
end

# The class label of a key, for its cost record. A `key_class` that throws costs the label, not
# the key.
function _class_of(key_class, key::DataKey)::String
    key_class === nothing && return ""
    try
        return String(string(key_class(key)))
    catch e
        e isa InterruptException && rethrow()
        return ""
    end
end

# JSON has no NaN: a reading that could not be taken is `nothing`.
_finite(x::Real) = isfinite(x) ? Float64(x) : nothing

# `keys` followed by the enqueued keys it does not already hold.
function _with_extra(keys::AbstractVector{DataKey}, extra::Vector{DataKey})
    isempty(extra) && return keys
    have = Set(canonical(k) for k in keys)
    return vcat(collect(keys), DataKey[k for k in extra if !(canonical(k) in have)])
end

export RunOpts, run!, run_loop!, manifest_root, load_manifest, todo_count, key_seconds
