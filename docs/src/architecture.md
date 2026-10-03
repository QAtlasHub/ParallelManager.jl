# Architecture

SweepRunner is a small set of files — each one concern, each one
module-scope piece. This page explains how they fit together and why.

## Layers

```
┌─────────────────────────────────────────────────┐
│  your project (templateHPC or similar)          │
├─────────────────────────────────────────────────┤
│  SweepRunner (this package)                 │
│  init_workers! / run! / Manifest /              │
│  EventLog / AtomicIO                            │
├─────────────────────┬───────────────────────────┤
│  DataVault          │  ParamIO                  │
│  Vault / save! /    │  ConfigSpec / DataKey /   │
│  load / is_done /   │  load / expand /          │
│  mark_done!         │  format_path / canonical  │
└─────────────────────┴───────────────────────────┘
```

Dependencies flow **downward only**. SweepRunner knows about
DataVault and ParamIO; neither of them know about SweepRunner.

## Why a separate layer?

`DataVault` already provides atomic single-file IO (`save!`, `mark_done!`)
and answers the "is this key complete?" question for one key at a time.
What it does not provide is:

1. **A rollup** that answers "are all N keys complete?" in O(1).
2. **Per-key advisory locks** so multiple masters can share a vault root
   without racing.
3. **Heartbeat-based stale-lock reclaim** so a `kill -9` does not wedge
   the queue forever.
4. **A structured event log** that is safe for concurrent append from
   multiple processes and hostile to per-item `println`.
5. **A uniform worker bootstrap** for `:threads` / `:distributed` / `:slurm`.

Putting those in `DataVault` would turn it into a parallel runtime; this
package keeps `DataVault` focused on "one file, one key, safely written"
and owns the coordination story separately.

## Module map

| File                                                | Responsibility                                                                                       |
| :-------------------------------------------------- | :--------------------------------------------------------------------------------------------------- |
| [`src/AtomicIO.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/AtomicIO.jl)   | `atomic_write` / `atomic_touch` — tmp + fsync + POSIX rename, NFS-safe                               |
| [`src/EventLog.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/EventLog.jl)   | JSONL structured log; single-write atomic lines for multi-process append safety                     |
| [`src/Manifest.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/Manifest.jl)   | Stage-level rollup of `canonical(key)` strings for O(1) early-skip                                   |
| _(per-key lock)_                                    | moved to DataVault's `.running` (`acquire_running!`, POSIX `link()`) as of v0.3; `Run.jl` calls into it                |
| [`src/InitWorkers.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/InitWorkers.jl) | Unified `:auto` / `:sequential` / `:threads` / `:distributed` / `:slurm` bootstrap                   |
| [`src/Run.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/Run.jl)             | [`run!(work_fn, vault, keys; opts)`](@ref SweepRunner.run!) facade                               |
| [`src/TaskTable.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/TaskTable.jl) | The master's table of a round's units (state, owner, progress) and the queue the dispatcher draws from |
| [`src/Progress.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/Progress.jl)   | `report_progress` / `resume_point`: how far a unit got, handed to the next attempt                  |
| [`src/Master.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/Master.jl)       | A master's identity and its workers'; `state_root(vault)`                                          |
| [`src/Status.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/Status.jl)       | The status file each master rewrites, and `read_status` / `print_status` to ask it from outside    |
| [`src/Locks.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/Locks.jl)         | `judge_lock`: ask the holder's master whether a `.running` is real; `locks`, `reap_dead_locks!`    |
| [`src/Control.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/Control.jl)     | `control!`: requests a running master applies (enqueue, cancel, stop, prioritise, resize, drain, pause); `should_stop` / `stop_point` |
| [`src/Pool.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/Pool.jl)           | `SizedPool`: workers sized to their keys (`KeyReq`), started where a node has room; `LocalSpawner`, `SlurmStepSpawner`; `plan_spawns` |
| [`src/Checkpoint.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/Checkpoint.jl) | A key's checkpoint inside `work_fn`: `load_checkpoint`, `save_checkpoint!`, `checkpoint_due`; `check_checkpoints` |
| [`src/Account.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/Account.jl)     | Where a job's core-hours went: computing (kept / lost), start-up, never started, idle by reason |
| [`src/Cost.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/Cost.jl)           | What a key cost: `key_costs`, `cost_summary`, the per-stage table, `measured_cost` / `measured_mem` |
| [`src/Campaign.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/Campaign.jl)   | A meta config naming the stages of a campaign: `load_campaign`, `validate_campaign`, `plan_campaign`, `run_campaign!`, `remaining_work` |
| [`src/Jobs.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/Jobs.jl)           | `Scheduler` (`SlurmScheduler`, `MockScheduler`), `JobPolicy`, `Ledger`, `decide` / `manage!`: submissions decided from what is left, inside a budget |
| [`src/CLI.jl`](https://github.com/QAtlasHub/SweepRunner.jl/blob/main/src/CLI.jl)             | `sweeprunner status|locks|costs|account|campaign|jobs|pause|resume|stop|cancel|prioritise|resize|drain|enqueue` (`bin/sweeprunner`) |

## Key identity: `canonical(::DataKey)`

`ParamIO.canonical` returns a deterministic, order-independent,
Julia-version-stable string form of a `DataKey`. `Manifest` uses it as
the index key, and the per-key `.running` lock uses it as the lock identity,
so both layers agree on the identity of each parameter point without touching
filesystem encodings.

## The `run!` pipeline

When you call `run!(work_fn, vault, keys)`, it does:

1. Open an [`EventLog`](@ref SweepRunner.EventLog) at
   `joinpath(vault.outdir, "events_<host>_<pid>.jsonl")` — one file per master.
2. Load the stage [`Manifest`](@ref SweepRunner.Manifest) and compute
   `todo = todo_keys(manifest, keys)`. If empty, emit `:skip_complete`
   and return.
3. Emit `:stage_start`.
4. Build the [`TaskTable`](@ref SweepRunner.TaskTable) — the master's one
   pass over the markers. A key a sibling finished since the manifest is
   settled; a lock whose holder is provably gone is removed; a key locked by
   a live sibling is held back (`:lock_busy`) and not dispatched; the progress
   recorded for partly-done units is attached. What remains is the queue.
5. Draw the queue. Each key is handed to a worker (or run on the master when
   there are none) **with its lock token and resume point**, and the row is
   settled with what comes back:
   - Acquire the per-key lock via `DataVault.acquire_running!` (atomic on
     NFS, POSIX `link()`), under the token the master named. If another
     master took it in the meantime, the key comes back `:lock_busy`.
   - Re-check `DataVault.is_done(vault, key)` **after** acquiring the lock
     — another master may have finished this key between our manifest
     read and lock acquisition.
   - Call `work_fn(key)` up to `opts.max_attempts` times, with a heartbeat
     child process (`DataVault.start_heartbeat`) refreshing `.running`. On
     success, `DataVault.save!` + `DataVault.mark_done!(vault, key, owner)` +
     `Manifest.add_complete!`; the owner form commits nothing if a sibling
     reclaimed the key meanwhile.
6. When the queue drains, ask once more about the keys that came back busy
   (their holder may have finished or died meanwhile) and requeue the free ones.
7. Merge the round's completions into the manifest. This also happens
   during the round, every `opts.manifest_interval` seconds, so a job killed
   at its wall clock leaves what it finished there.
8. Emit `:stage_done` and return the aggregate counts.

### Who holds the task state

The markers on disk stay the durable record and the cross-job lock: several
masters share one vault, and any of them can be killed at its wall clock. What
changed in 0.6.9 is who READS them. The master reads them once per round and
holds the table; a worker does the key it was handed and reports back. It does
not explore.

The same goes for progress inside a unit. A `work_fn` made of steps calls
[`report_progress`](@ref SweepRunner.report_progress) after each one and
[`resume_point`](@ref SweepRunner.resume_point) at the start, instead of
probing its own outputs step by step:

```julia
function work_fn(key)
    p = SweepRunner.resume_point()
    for seg in (p === nothing ? 1 : p.step + 1):nseg
        run_segment!(key, seg)                    # writes its own checkpoint
        SweepRunner.report_progress(seg; of=nseg)
    end
    return collect_result(key)
end
```

A worker that dies gives its key back with what it had reported, and the
master releases the lock it named at once rather than leaving it for
`stale_after`.

## Concurrency model

```
time →

master A:  acquire(K1)  work(K1)  release(K1)  acquire(K2)  busy → next  acquire(K3)  work(K3)...
master B:                                      acquire(K2)  work(K2)  release(K2)  busy → next ...
```

Both masters iterate the same `todo`. The `.running` (POSIX `link()`) lock ensures only
one enters `work_fn` for any given key at any time. A master that tries
to lock a key another master already owns simply logs `:lock_busy` and
moves on — no blocking, no waiting, no central queue.

Two things keep this robust against crashes:

1. **Heartbeat + stale reclaim.** A child process of the live holder
   rewrites the lock's `heartbeat_unix=` every `heartbeat_interval`, for as
   long as the holder's pid lives — also while `work_fn` never yields, which
   an in-process task did not survive. If the holder dies, the heartbeat
   stops. A master that starts later asks the holder's master, the scheduler
   and the pid whether the holder is alive, and removes the lock at once when
   the answer is no; a lock a reporting master lists as held is left alone
   whatever its age. Only where nobody can be asked does the heartbeat's age
   decide: after `stale_after` the next contender reclaims the lock under
   DataVault's reclaim mutex (see DataVault's README, "ロックの規約").
2. **Post-lock `is_done` re-check.** Even on the happy path, two masters
   can start the loop with overlapping `todo`. The re-check inside the
   locked critical section ensures the second master notices the work
   is already done and skips it — no duplicate `work_fn` calls ever hit
   the physics code.

## Why no `println`

FiniteTemperature.jl used to emit ~300 MB of log files per job. Tracing
showed they came from per-item `println` calls that walked 3600 `.done`
files and announced each one's state. Switching to aggregated events
via `EventLog` collapses that to ~2 events per key plus a handful of
per-stage events, each a structured JSON line, totaling well under 1 MB
for typical jobs.

SweepRunner's public API **does not include a per-item `println`**.
Adding one is considered a regression. Use [`log_event`](@ref SweepRunner.log_event)
with one of the standard event kinds documented on [`EventLog`](@ref SweepRunner.EventLog).

## Campaigns: stages are declared, not composed

`run!` runs one stage. A campaign is many stages of many studies, and which of
them run, in what order and under which filters is described in one meta
config ([`load_campaign`](@ref SweepRunner.load_campaign)), not in an entry
script and a set of environment variables. The dependency between stages is
declared there (`needs`) and checked as "every key of the needed stage is
done" before the dependent stage starts; SweepRunner still does not know which
dependent key reads which needed key. See the campaign guide.

## Why no Stage / DAG

An earlier draft of this package considered a `Stage{I,O}` type with
`|>` composition for multi-phase workflows. It was dropped because the
single `DataVault.load(phase1_vault, key)` line inside a `work_fn`
already eliminates the cross-phase `逆参照` failure mode (where phase2
builds filesystem paths for phase1 by hand), and the added abstraction
adds learning cost without paying for itself at the current scale.

If a pattern emerges across multiple projects for stage composition,
the right layer to build it on is `DataVault.load(parent_vault, key)`
in a thin helper — not a new abstract type in SweepRunner.
