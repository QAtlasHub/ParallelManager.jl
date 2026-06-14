# Architecture

ParallelManager is a small set of files — each one concern, each one
module-scope piece. This page explains how they fit together and why.

## Layers

```
┌─────────────────────────────────────────────────┐
│  your project (templateHPC or similar)          │
├─────────────────────────────────────────────────┤
│  ParallelManager (this package)                 │
│  init_workers! / run! / Manifest /              │
│  EventLog / AtomicIO                            │
├─────────────────────┬───────────────────────────┤
│  DataVault          │  ParamIO                  │
│  Vault / save! /    │  ConfigSpec / DataKey /   │
│  load / is_done /   │  load / expand /          │
│  mark_done!         │  format_path / canonical  │
└─────────────────────┴───────────────────────────┘
```

Dependencies flow **downward only**. ParallelManager knows about
DataVault and ParamIO; neither of them know about ParallelManager.

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
| [`src/AtomicIO.jl`](https://github.com/sotashimozono/ParallelManager.jl/blob/main/src/AtomicIO.jl)   | `atomic_write` / `atomic_touch` — tmp + fsync + POSIX rename, NFS-safe                               |
| [`src/EventLog.jl`](https://github.com/sotashimozono/ParallelManager.jl/blob/main/src/EventLog.jl)   | JSONL structured log; single-write atomic lines for multi-process append safety                     |
| [`src/Manifest.jl`](https://github.com/sotashimozono/ParallelManager.jl/blob/main/src/Manifest.jl)   | Stage-level rollup of `canonical(key)` strings for O(1) early-skip                                   |
| _(per-key lock)_                                    | moved to DataVault's `.running` (`acquire_running!`, POSIX `link()`) as of v0.3; `Run.jl` calls into it                |
| [`src/InitWorkers.jl`](https://github.com/sotashimozono/ParallelManager.jl/blob/main/src/InitWorkers.jl) | Unified `:auto` / `:sequential` / `:threads` / `:distributed` / `:slurm` bootstrap                   |
| [`src/Run.jl`](https://github.com/sotashimozono/ParallelManager.jl/blob/main/src/Run.jl)             | [`run!(work_fn, vault, keys; opts)`](@ref ParallelManager.run!) facade                               |

## Key identity: `canonical(::DataKey)`

`ParamIO.canonical` returns a deterministic, order-independent,
Julia-version-stable string form of a `DataKey`. `Manifest` uses it as
the index key, and the per-key `.running` lock uses it as the lock identity,
so both layers agree on the identity of each parameter point without touching
filesystem encodings.

## The `run!` pipeline

When you call `run!(work_fn, vault, keys)`, it does:

1. Open an [`EventLog`](@ref ParallelManager.EventLog) at
   `joinpath(vault.outdir, "events.jsonl")`.
2. Load the stage [`Manifest`](@ref ParallelManager.Manifest) and compute
   `todo = todo_keys(manifest, keys)`. If empty, emit `:skip_complete`
   and return.
3. Emit `:stage_start`.
4. For each key in `todo`:
   - Acquire the per-key lock via `DataVault.acquire_running!` (atomic on
     NFS, POSIX `link()`). If another master holds a fresh `.running`,
     emit `:lock_busy` and move on.
   - Re-check `DataVault.is_done(vault, key)` **after** acquiring the lock
     — another master may have finished this key between our manifest
     read and lock acquisition.
   - Call `work_fn(key)` up to `opts.max_attempts` times, with a heartbeat
     task refreshing `.running`. On success, `DataVault.save!` +
     `DataVault.mark_done!` + `Manifest.add_complete!`.
5. `save_manifest(manifest)`.
6. Emit `:stage_done` and return the aggregate counts.

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

1. **Heartbeat + stale reclaim.** The live holder touches `heartbeat` on
   a timer. If a holder dies, its `heartbeat` mtime stops advancing; the
   next contender sees the lock as stale (by either heartbeat age or lock-dir
   age, depending on whether the holder made it past initial write), and
   reclaims via `mv lock lock.dead.X` + `rm -rf lock.dead.X`.
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

ParallelManager's public API **does not include a per-item `println`**.
Adding one is considered a regression. Use [`log_event`](@ref ParallelManager.log_event)
with one of the standard event kinds documented on [`EventLog`](@ref ParallelManager.EventLog).

## Why no Stage / DAG

An earlier draft of this package considered a `Stage{I,O}` type with
`|>` composition for multi-phase workflows. It was dropped because the
single `DataVault.load(phase1_vault, key)` line inside a `work_fn`
already eliminates the cross-phase `逆参照` failure mode (where phase2
builds filesystem paths for phase1 by hand), and the added abstraction
adds learning cost without paying for itself at the current scale.

If a pattern emerges across multiple projects for stage composition,
the right layer to build it on is `DataVault.load(parent_vault, key)`
in a thin helper — not a new abstract type in ParallelManager.
