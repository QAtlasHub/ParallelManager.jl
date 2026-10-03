# CLAUDE.md — SweepRunner.jl

**Layer 3 (top) of the infra HPC stack** (ParamIO → DataVault →
SweepRunner): `run!(work_fn, vault, keys)` is the runtime that ties layers 1
and 2 together with parallel dispatch, advisory locking, manifest early-skip,
structured event logging, retry, and crash recovery. See [`../CLAUDE.md`](../CLAUDE.md)
for how the three layers fit together.

## Role / public API

- `init_workers!(mode=:auto)` — bootstrap the backend
  (`:sequential`/`:threads`/`:distributed`/`:slurm`, chosen from env vars).
- `run!(work_fn, vault, keys; opts=RunOpts())` — execute; returns a counter
  NamedTuple `(stage, done, err, skipped, total, …)`.
- `run_loop!(...)` — re-scan until the sweep is fully done; the production driver
  (picks up keys freed by crashed sibling masters).
- `RunOpts(; max_attempts, stale_after, heartbeat_interval, stop_flag, status_interval,
  control_interval)`.
- `read_status(vault | outdir)` / `print_status` / `bin/sweeprunner status <outdir>` — what every
  master is doing (tasks, workers planned/launched/joined/busy, per-worker key, CPU, RSS), read
  from the status file each master rewrites. A spawner reports `note_workers!(planned=, launched=)`.
- `load_campaign(meta.toml)` / `validate_campaign` / `plan_campaign` / `run_campaign!(open_stage,
  campaign; profile, cost)` — a META config names the per-stage configs of a campaign, their
  order (`needs`, `priority`), which studies are `enabled`, and per-job-kind `[profile.*]`
  filters. The application supplies `open_stage(stage) -> (; work_fn, …)`.
  `bin/sweeprunner campaign <meta.toml>` validates and prints the plan.
- `control!(vault | outdir, op; …)` / `bin/sweeprunner <op> <outdir>` — requests to a RUNNING
  master: `:enqueue`, `:cancel`, `:stop` (with `grace`), `:prioritise`, `:resize`, `:drain`,
  `:pause`, `:resume`. One file per request under `state_root/control`, read by every master on
  the vault, acknowledged and logged with who asked.
- `locks(vault[, keys] | outdir)` / `bin/sweeprunner locks <outdir>` — every `.running`, who holds
  it and whether its holder's MASTER says it is held or dead (`judge_lock`). `run!` reconciles
  the locks before it builds its queue; `reap_dead_locks!` does it without running anything.

## The `work_fn` contract — read this

- **`work_fn` is a PURE `(DataKey) -> Dict`. It RETURNS the payload; `run!`
  calls `DataVault.save!` + `mark_done!` for you.** Never `save!` inside
  `work_fn`. Returning a non-`Dict` is a runtime error.
- Read params by the **DOTTED** key: `key.params["system.N"]`.
- Depend only on `key` (broadcast shared config with `@everywhere const`),
  or the function breaks under `:distributed`.
- **Worker module loading is automatic.** `run!` `using`s `ParamIO`/`DataVault`/`SweepRunner`
  in `Main` on every worker before fan-out, so a sweep no longer dies with a cryptic
  `KeyError: <Module> not found` on the first dispatched key (a failure only ever seen on real Slurm).
  Name any *additional* installed module your `work_fn` needs — its own package, or a stdlib like
  `Statistics` — via `run!(…; load=MyModule)` (also accepts `load=[A, B]`, a `Symbol`, or a
  `String`). This replaces the hand-rolled `for w in workers(); remotecall_fetch(w, …, :(using …));
  end` broadcast. Caveat: a work *function* defined inline in the script (not in a package) must
  still be `@everywhere function …`; `load=` resolves installed modules, not `Main` submodules.
- **A `work_fn` made of steps asks, it does not probe.** `SweepRunner.resume_point()` returns
  the last step an earlier attempt reported (on any worker, of any job) and
  `SweepRunner.report_progress(step; of=n)` records one. The master read the progress stamps when
  it built its task table and handed the value over with the key; a `work_fn` that walks its own
  outputs to find where to resume is doing the scan the table exists to remove.
- **A `work_fn` that can leave part-way says where: `SweepRunner.stop_point()`** after each
  step whose state is on disk. It throws `StopRequested` when the job's `stop_flag` / `deadline`
  or a `control!(…, :stop)` covers this key; `run!` spends no attempt on it. Without it a stop is
  only read between keys.
- **No per-item `println`** — structured events go through `EventLog` (JSONL)
  only. This is deliberate (the old loop generated 300 MB of per-item logs).

## Where to look for usage

- **`examples/`** — runnable end-to-end: `compute.jl` (phase 1), `refine.jl`
  (phase chaining), `summarize.jl` (read-back), `batch/submit_slurm.sh`. Start here.
- `docs/src/quickstart.md`, `README.md`.
- Real production usage: `apps/ReducedEnvExperiments.jl/projects/*/scripts/compute.jl`.

## Module layout — one file, one concern

`AtomicIO` (atomic write) · `EventLog` (JSONL) · `Manifest` (O(1) early-skip) ·
`InitWorkers` (backend bootstrap) · `Run` (the `run!` facade) · `TaskTable` (the master's table
of a round's units and its queue) · `Progress` (`report_progress` / `resume_point`) · `Status`
(the status file and its readers) · `Locks` (ask the holder's master) · `Control` (requests to a
running master) · `Campaign` (the meta config) · `CLI`. Each is usable
independently. As of v0.3 the per-key advisory lock lives entirely in
**DataVault's `.running` markers** (`acquire_running!`); `run!` calls into it
rather than maintaining its own `locks/` tree.

## Invariants when changing this package

- The `Manifest` is **monotonic** (keys only added); multi-master coordination
  uses `mkdir` / atomic `rename` only (NFS-safe) — no `flock`, no central service.
- Run the test FILE you touched locally (`julia --project=<throwaway env> test/run/test_x.jl`),
  then push: the full suite is CI's, sharded, on every PR and on pushes to `main` and `next`.
- A release is collected on `next`: feature PRs target `next`, and one PR `next → main` carries
  the version bump.
