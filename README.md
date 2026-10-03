# SweepRunner.jl

[![docs: dev](https://img.shields.io/badge/docs-dev-purple.svg)](https://qatlashub.github.io/SweepRunner.jl/dev/)
[![Julia](https://img.shields.io/badge/julia-v1.11+-9558b2.svg)](https://julialang.org)
[![Code Style: Blue](https://img.shields.io/badge/Code%20Style-Blue-4495d1.svg)](https://github.com/invenia/BlueStyle)

[![codecov](https://codecov.io/gh/QAtlasHub/SweepRunner.jl/graph/badge.svg?token=0kGBejbpL8)](https://codecov.io/gh/QAtlasHub/SweepRunner.jl)
[![Build Status](https://github.com/QAtlasHub/SweepRunner.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/QAtlasHub/SweepRunner.jl/actions/workflows/CI.yml?query=branch%3Amain)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Running a parameter sweep on a cluster is mostly bookkeeping. The physics is a
function of one parameter point; everything around it is not. Working out which
points are still missing, dispatching them to SLURM or `Distributed` or threads,
writing each result without corrupting it, and continuing after the scheduler
kills the job — that layer gets rewritten once per project and is never quite
right.

SweepRunner is that layer, factored out. `run!` takes a store, a list of
parameter keys, and a function that computes one key. It skips the keys that are
already finished, runs the rest, and records what happened. Two `julia`
processes can point at the same store without computing the same key twice, and
a run that is killed is continued by the next one rather than left half-done.

The parameter keys come from [ParamIO.jl](https://github.com/QAtlasHub/ParamIO.jl)
and the store from [DataVault.jl](https://github.com/QAtlasHub/DataVault.jl).

## Highlights

- **Multi-master safe** — several `julia` processes can hit the same vault
  root without double-executing any key. Locking is delegated to
  `DataVault.acquire_running!`, which uses POSIX `link()` for an atomic
  "create iff not exists" that works on NFS — the `.running` marker is the lock.
- **Crash recovery** — `kill -9` a master mid-run and the next `run!` picks up
  where it left off: `DataVault` writes a heartbeat into `.running`; the next job
  removes the locks whose holder is gone and reclaims the ones nobody answers for after
  `stale_after`.
- **Early skip** — full-done re-runs take O(1) filesystem operations
  (a single `manifest.jld2` read), not O(N) per-key `.done` stats.
  A 3600-key warm re-run takes milliseconds; the test suite bounds it at 500 ms.
- **Structured events** — JSONL event log atomic across concurrent writers;
  per-item `println` is a non-goal, by design. Every lock acquisition writes a
  flushed `key_acquired` line, so a run that a `kill -9` truncated still says
  which keys it had claimed; the status tree cannot, because a key that was
  claimed and never finished leaves no `.done` and no `.failed`.
- **A stop flag and a deadline** — `RunOpts(stop_flag=...)` is read between keys
  and so is `RunOpts(deadline=time() + 25*60)`. The difference is when you set
  it: a deadline is budgeted in advance, so a batch job can subtract its longest
  expected key and reserve the tail of its allocation for the summary it needs
  to print. `run!` reports which one fired as `result.stopped_by`. A `work_fn`
  that calls `SweepRunner.stop_point()` at its safe points sees both from
  inside a key and leaves there, at no attempt.
- **The master holds the task table** — it reads the markers once per round,
  does not dispatch a key a live sibling holds, and hands each worker its key
  together with the lock token and the last progress recorded for it.
  `report_progress(step; of)` / `resume_point()` replace a `work_fn` probing its
  own outputs step by step. A worker that dies has its lock released at once.
- **Ask a running sweep** — every master rewrites a status file;
  `bin/sweeprunner status <outdir>` (or `read_status`) shows tasks by state,
  workers planned / launched / joined / busy, cores in use, nodes with no
  worker, and per worker its key, CPU utilisation and RSS. A ramp-up that
  stalls is a `workers_short` warning, not silence.
- **Whose lock is this** — a `.running` is judged by asking the holder's master,
  which lists the locks it has out; a job that starts reconciles the locks it
  finds (`locks: 260 held by 3 job(s), 17 reaped (4 dead job(s)) …`), and
  `bin/sweeprunner locks <outdir>` answers at any time.
- **Change a sweep while it runs** — `control!(vault, :cancel; select=…)`,
  `:enqueue`, `:stop` (with a grace, after which the unit is cut and its lock
  released), `:prioritise`, `:resize`, `:drain`, `:pause` / `:resume`; also as
  `bin/sweeprunner <op> <outdir>`. Requests are files every master on the vault
  reads, acknowledges and logs with who asked.
- **One file for a campaign** — a meta config names the per-stage configs, what
  each stage needs, priorities, which studies are enabled and per-job-kind
  profiles; `run_campaign!` runs it and records which file and profile a job
  ran. `bin/sweeprunner campaign <meta.toml>` validates and prints the plan.
- **Submissions decided from what is left** — a scheduler interface (SLURM and a
  mock), a policy with a hard node-hour budget, and `decide` / `manage!`:
  nothing runnable means nothing submitted. Dry run by default.
- **One entry point for all parallel modes** — `init_workers!(mode=:auto)`
  dispatches to `:sequential` / `:threads` / `:distributed` / `:slurm`
  depending on environment.
- **Pure work functions** — your physics is a plain
  `(DataKey) -> Dict`, IO/locking/logging live in the runtime.
- **Worker affinity** — `run!(...; affinity = k -> ...)` makes a free worker
  prefer a key whose group it has already handled, so worker-local memoisation
  of a shared setup is hit instead of reloaded. A preference, never a partition:
  no worker idles while a key is pending. Measured, 24 keys over 2 groups on 8
  workers: **8 group changes without it, 0 with.**
- **Prerequisite stages** — `run!` locks the KEY, so work SHARED between keys
  has nowhere to live but inside `work_fn`, where every worker that wants a
  setup not yet on disk builds it itself. A `Prerequisite` makes that setup its
  own key space, run to completion first, with the same locking, resume and
  provenance. Measured, 8 concurrent processes over 16 keys sharing 2 setups:
  **16 builds inside `work_fn`, 2 with a prerequisite.**

## Quick start

```julia
using ParamIO, DataVault, SweepRunner

# 1. Load the parameter sweep
spec  = ParamIO.load("config.toml")
keys  = ParamIO.expand(spec)
vault = DataVault.Vault("config.toml"; run="phase1")

# 2. Bootstrap workers (auto-detects SLURM / threads / sequential)
SweepRunner.init_workers!(mode=:auto)

# 3. Describe the work as a pure function
work_fn = key -> Dict{String,Any}("spectrum" => my_dmrg(key.params["N"]))

# 4. Run — manifest-aware, lock-safe, crash-recoverable
SweepRunner.run!(work_fn, vault, keys)
```

Re-running the same script after completion: `:skip_complete` is logged and
the process exits within milliseconds regardless of `length(keys)`.

### Shared setup

When many keys need one expensive thing, give that thing its own key space:

```julia
using ParamIO, DataVault, SweepRunner

spec  = ParamIO.load("config.toml")
main  = DataVault.Vault("config.toml"; run="dependent")
prep  = DataVault.Vault("config.toml"; run="setup")

# The axes the setup actually depends on. ParamIO.project derives this from the
# same spec, so the two key spaces cannot drift apart by hand.
derived = ParamIO.expand(ParamIO.project(spec, ["system.L", "model.lambda", "thermal.beta"]))

run_loop!(work_fn, main, ParamIO.expand(spec);
          prerequisite = Prerequisite(prep_fn, prep, derived),
          affinity     = k -> ParamIO.param(k, "system.L"),
          opts         = RunOpts(deadline = time() + 25*60))
```

`prerequisite` removes the duplicated *build*; `affinity` removes the repeated *load* of what it
built. The second only matters once the first is in place.

The prerequisite is a **barrier**: `run_loop!` does not start the dependent stage until every setup
key is done, and if one cannot be built it does not start it at all. The dependency is one level
deep and resolved inside `work_fn`, so this is "all of the setup, then all of the dependents", not
a DAG.

### Shared setup without a barrier — artifacts

When the setup depends on a subset of the axes, declaring it in the config
(`[artifacts.<name>] depends_on = [...]`, ParamIO ≥ 0.4.11) lets `work_fn` build it on demand
and every other key reuse it, with no second vault and no barrier:

```julia
work_fn = k -> begin
    gs = DataVault.artifact!(vault, :ground_state, k; wait=false) do akey   # a miss builds
        prepare(param(akey, "system.L"))
    end
    Dict{String,Any}("x" => respond(gs, k))
end
run!(work_fn, vault, keys; affinity = artifact_affinity(vault, :ground_state))
```

- `artifact_affinity` keeps keys that share an artifact on one worker and starts distinct
  artifacts on distinct workers.
- With `wait=false`, a key whose artifact another worker or job is still building throws
  `DataVault.ArtifactBusy`; `run!` logs `:artifact_busy`, **defers the key without spending an
  attempt**, and re-dispatches it once the pass drains (`:deferred_round`; after a pass that
  finished nothing it first waits `RunOpts(defer_poll=30.0)`). A key still deferred when the run
  stops is counted with `busy`. With `wait=true` (the default) the worker simply blocks until the
  artifact exists — the better choice when there are no more keys than workers.

The artifact lives outside the run (`{outdir}/artifacts/...`), so the next job and the next run
under the same `outdir` reuse it too. `Prerequisite` remains for setups that must be complete
before anything else starts.

## Phase chaining without `Stage` / `DAG`

A dependent stage loads its parent's output inside the work function using
one line of `DataVault.load`. There is **no** path-building helper, no
`Stage` abstraction, no `DAG` — just the regular work_fn pattern.

```julia
phase1_vault = DataVault.Vault(config_path; run="phase1")
phase2_vault = DataVault.Vault(config_path; run="phase2")

work_fn = key -> begin
    mps = DataVault.load(phase1_vault, key)   # ← the one line
    return Dict{String,Any}("energy" => measure_thermal(mps))
end

SweepRunner.run!(work_fn, phase2_vault, keys)
```

This is the canonical replacement for the `p2_phase1_mps_path`-style string
path builders that leak phase1's storage layout into phase2's code.

## Module layout

| File | Responsibility |
| --- | --- |
| [`src/AtomicIO.jl`](src/AtomicIO.jl) | `atomic_write` / `atomic_touch` — tmp + fsync + POSIX rename, NFS-safe |
| [`src/EventLog.jl`](src/EventLog.jl) | JSONL structured log, single-write atomic lines for concurrent appends |
| [`src/Manifest.jl`](src/Manifest.jl) | Stage-level rollup of `canonical(key)` strings for O(1) early-skip |
| [`src/InitWorkers.jl`](src/InitWorkers.jl) | Unified `:auto` / `:sequential` / `:threads` / `:distributed` / `:slurm` bootstrap |
| [`src/Run.jl`](src/Run.jl) | `run!(work_fn, vault, keys; opts)` facade that ties everything to `DataVault` |
| [`src/TaskTable.jl`](src/TaskTable.jl) | The master's table of a round's units (state, owner, progress) and its queue |
| [`src/Progress.jl`](src/Progress.jl) | `report_progress` / `resume_point`: how far a unit got, handed to the next attempt |
| [`src/Status.jl`](src/Status.jl) | The status file each master rewrites; `read_status` / `print_status` |
| [`src/Locks.jl`](src/Locks.jl) | `judge_lock` (ask the holder's master), `locks`, `reap_dead_locks!` |
| [`src/Control.jl`](src/Control.jl) | `control!` requests to a running master; `should_stop` / `stop_point` |
| [`src/Campaign.jl`](src/Campaign.jl) | A meta config: `load_campaign`, `validate_campaign`, `plan_campaign`, `run_campaign!` |
| [`src/Jobs.jl`](src/Jobs.jl) | `Scheduler`, `JobPolicy`, `Ledger`, `decide` / `manage!` |
| [`src/Master.jl`](src/Master.jl) | A master's identity and its workers'; `state_root(vault)` |
| [`src/Liveness.jl`](src/Liveness.jl) | Is a process or a Slurm job still there: what a lock's holder is asked through |
| [`src/Pool.jl`](src/Pool.jl) | `SizedPool`: workers sized to their keys; `LocalSpawner`, `SlurmStepSpawner` |
| [`src/Checkpoint.jl`](src/Checkpoint.jl) | A key's checkpoint inside `work_fn`: `load_checkpoint`, `save_checkpoint!`, `checkpoint_due` |
| [`src/Cost.jl`](src/Cost.jl) | What a key cost: `key_costs`, `cost_summary`, `measured_cost` |
| [`src/Account.jl`](src/Account.jl) | Where a job's core-hours went |
| [`src/Observe.jl`](src/Observe.jl) | One source observation per process, carried by every `.done` it writes |
| [`src/CLI.jl`](src/CLI.jl) | `bin/sweeprunner status|locks|costs|account|campaign|jobs|pause|stop|cancel|…` |
| [`src/Preflight.jl`](src/Preflight.jl) | `check_injective!` / `check_opens!` / `on_grid` — refuse a campaign *before* it burns compute |

Each module is one file, one concern. They can be used independently
(e.g. `atomic_write` + `EventLog` without `run!`).

## Pain points it answers

Built from direct experience with the old-style HPC loop pattern used in
`FiniteTemperature.jl`:

| Pain | This package's answer |
| --- | --- |
| `.done` files rescanned every job (3600 files, ~10 min) | `Manifest` rollup, one JLD2 read (milliseconds) |
| 300 MB of per-item `println` logs | `EventLog` (JSONL), per-item `println` is not part of the API |
| Killed samples silently wedge the queue | Heartbeat in `.running`; the next job asks who holds each lock (`judge_lock`) and reaps the dead ones, `stale_after` as the last resort |
| Multiple masters double-execute the same key | `DataVault.acquire_running!` (POSIX `link()`) + post-lock `is_done` re-check |
| Half-written JLD2 files after crash | `atomic_write` (tmp + fsync + rename) |
| Every project reinvents SLURM / Distributed bootstrap | `init_workers!(mode=:auto)` absorbs the pattern |
| A 72-node job ran at 48% of its cores and nothing said so | Status file + `workers_short` warning; `sweeprunner status` |
| Every worker re-scans `seg 1 already done. Skipping.` | The master's task table hands out the key with its resume point |
| Locks of ended jobs sit on disk for hours; a live job proves nothing about a key | `judge_lock` asks the holder's master; reconciliation at job start |
| Adding work or stopping a study means cancel and resubmit | `control!` requests to the running master |
| Which configs run is an entry script, env vars and a magic file | One meta config (`load_campaign`) with profiles |
| A resubmit loop keeps submitting after the eligible work ran out | `decide`: nothing runnable, nothing submitted; a hard budget |

## Installation

```julia
pkg> add SweepRunner
```

Its dependencies `ParamIO.jl` and `DataVault.jl` are in the General registry too, so nothing
needs a `[sources]` entry.

Requires Julia v1.11+.

## Tests

The suite runs on CI in four shards (one directory per module under `test/`), including:

- `atomicio/` — atomic write, exception cleanup, concurrent writers
- `eventlog/` — JSON roundtrip, 50-task × 40-event concurrent write
- `manifest/` — save/load, `todo_keys`, 3600-key bench, corrupted file
- `init_workers/` — `:auto`, `:sequential`, `:threads`, `:slurm` env reading
- `run/` — minimal, manifest early-skip, 8-master race under a fast heartbeat, retry, gave_up

## See also

- [ParamIO.jl](https://github.com/QAtlasHub/ParamIO.jl) — config TOML parsing and `DataKey` enumeration
- [DataVault.jl](https://github.com/QAtlasHub/DataVault.jl) — `Vault` struct, atomic JLD2 save, `.done` markers
- [templateHPC.jl](https://github.com/QAtlasHub/templateHPC.jl) — clone-to-start scaffold wiring all three together

## License

MIT. See [`LICENSE`](LICENSE).
