# CLAUDE.md — ParallelManager.jl

**Layer 3 (top) of the infra HPC stack** (ParamIO → DataVault →
ParallelManager): `run!(work_fn, vault, keys)` is the runtime that ties layers 1
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
- `RunOpts(; max_attempts, stale_after, heartbeat_interval, stop_flag)`.

## The `work_fn` contract — read this

- **`work_fn` is a PURE `(DataKey) -> Dict`. It RETURNS the payload; `run!`
  calls `DataVault.save!` + `mark_done!` for you.** Never `save!` inside
  `work_fn`. Returning a non-`Dict` is a runtime error.
- Read params by the **DOTTED** key: `key.params["system.N"]`.
- Depend only on `key` (broadcast shared config with `@everywhere const`),
  or the function breaks under `:distributed`.
- **Worker module loading is automatic.** `run!` `using`s `ParamIO`/`DataVault`/`ParallelManager`
  in `Main` on every worker before fan-out, so a sweep no longer dies with a cryptic
  `KeyError: <Module> not found` on the first pmap task (a failure only ever seen on real Slurm).
  Name any *additional* installed module your `work_fn` needs — its own package, or a stdlib like
  `Statistics` — via `run!(…; load=MyModule)` (also accepts `load=[A, B]`, a `Symbol`, or a
  `String`). This replaces the hand-rolled `for w in workers(); remotecall_fetch(w, …, :(using …));
  end` broadcast. Caveat: a work *function* defined inline in the script (not in a package) must
  still be `@everywhere function …`; `load=` resolves installed modules, not `Main` submodules.
- **No per-item `println`** — structured events go through `EventLog` (JSONL)
  only. This is deliberate (the old loop generated 300 MB of per-item logs).

## Where to look for usage

- **`examples/`** — runnable end-to-end: `compute.jl` (phase 1), `refine.jl`
  (phase chaining), `summarize.jl` (read-back), `batch/submit_slurm.sh`. Start here.
- `docs/src/quickstart.md`, `README.md`.
- Real production usage: `apps/ReducedEnvExperiments.jl/projects/*/scripts/compute.jl`.

## Module layout — one file, one concern

`AtomicIO` (atomic write) · `EventLog` (JSONL) · `Manifest` (O(1) early-skip) ·
`InitWorkers` (backend bootstrap) · `Run` (the `run!` facade). Each is usable
independently. As of v0.3 the per-key advisory lock lives entirely in
**DataVault's `.running` markers** (`acquire_running!`); `run!` calls into it
rather than maintaining its own `locks/` tree.

## Invariants when changing this package

- The `Manifest` is **monotonic** (keys only added); multi-master coordination
  uses `mkdir` / atomic `rename` only (NFS-safe) — no `flock`, no central service.
- Run the suite locally before pushing.
