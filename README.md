# ParallelManager.jl

[![docs: stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://codes.sota-shimozono.com/ParallelManager.jl/stable/)
[![docs: dev](https://img.shields.io/badge/docs-dev-purple.svg)](https://codes.sota-shimozono.com/ParallelManager.jl/dev/)
[![Julia](https://img.shields.io/badge/julia-v1.11+-9558b2.svg)](https://julialang.org)
[![Code Style: Blue](https://img.shields.io/badge/Code%20Style-Blue-4495d1.svg)](https://github.com/invenia/BlueStyle)

[![codecov](https://codecov.io/gh/QAtlasHub/ParallelManager.jl/graph/badge.svg?token=0kGBejbpL8)](https://codecov.io/gh/QAtlasHub/ParallelManager.jl)
[![Build Status](https://github.com/QAtlasHub/ParallelManager.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/QAtlasHub/ParallelManager.jl/actions/workflows/CI.yml?query=branch%3Amain)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**HPC experiment runtime for Julia** — multi-master safe, crash-recoverable,
`Pkg.test()`-fast. Wraps [ParamIO.jl](https://github.com/QAtlasHub/ParamIO.jl)
and [DataVault.jl](https://github.com/QAtlasHub/DataVault.jl) with a
unified `run!` that handles parallel dispatch, advisory locking, `.done`
rollups, structured event logging, and retry.

Designed to replace the recurring "glue" layer that every HPC research
project re-invents: the script that loops over parameter points, decides
which ones still need work, runs them on SLURM / Distributed / threads,
survives kills, and logs what happened.

## Highlights

- **Multi-master safe** — several `julia` processes can hit the same vault
  root without double-executing any key (mkdir-based advisory lock).
- **Crash recovery** — `kill -9` a master mid-run and the next `run!` picks
  up where it left off via heartbeat-based stale lock reclaim.
- **Early skip** — full-done re-runs take O(1) filesystem operations
  (a single `manifest.jld2` read), not O(N) per-key `.done` stats.
  Benchmark: 3600 keys warm re-run ≈ 3.5 ms.
- **Structured events** — JSONL event log atomic across concurrent writers;
  per-item `println` is a non-goal, by design.
- **One entry point for all parallel modes** — `init_workers!(mode=:auto)`
  dispatches to `:sequential` / `:threads` / `:distributed` / `:slurm`
  depending on environment.
- **Pure work functions** — your physics is a plain
  `(DataKey) -> Dict`, IO/locking/logging live in the runtime.

## Quick start

```julia
using ParamIO, DataVault, ParallelManager

# 1. Load the parameter sweep
spec  = ParamIO.load("config.toml")
keys  = ParamIO.expand(spec)
vault = DataVault.Vault("config.toml"; run="phase1")

# 2. Bootstrap workers (auto-detects SLURM / threads / sequential)
ParallelManager.init_workers!(mode=:auto)

# 3. Describe the work as a pure function
work_fn = key -> Dict{String,Any}("spectrum" => my_dmrg(key.params["N"]))

# 4. Run — manifest-aware, lock-safe, crash-recoverable
ParallelManager.run!(work_fn, vault, keys)
```

Re-running the same script after completion: `:skip_complete` is logged and
the process exits within milliseconds regardless of `length(keys)`.

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

ParallelManager.run!(work_fn, phase2_vault, keys)
```

This is the canonical replacement for the `p2_phase1_mps_path`-style string
path builders that leak phase1's storage layout into phase2's code.

## Module layout

| File | Responsibility |
| --- | --- |
| [`src/AtomicIO.jl`](src/AtomicIO.jl) | `atomic_write` / `atomic_touch` — tmp + fsync + POSIX rename, NFS-safe |
| [`src/EventLog.jl`](src/EventLog.jl) | JSONL structured log, single-write atomic lines for concurrent appends |
| [`src/Manifest.jl`](src/Manifest.jl) | Stage-level rollup of `canonical(key)` strings for O(1) early-skip |
| [`src/KeyLock.jl`](src/KeyLock.jl) | `mkdir` advisory lock + heartbeat + stale reclaim |
| [`src/InitWorkers.jl`](src/InitWorkers.jl) | Unified `:auto` / `:sequential` / `:threads` / `:distributed` / `:slurm` bootstrap |
| [`src/Run.jl`](src/Run.jl) | `run!(work_fn, vault, keys; opts)` facade that ties everything to `DataVault` |

Each module is one file, one concern. They can be used independently
(e.g. `atomic_write` + `EventLog` without `run!`).

## Pain points it answers

Built from direct experience with the old-style HPC loop pattern used in
`FiniteTemperature.jl`:

| Pain | This package's answer |
| --- | --- |
| `.done` files rescanned every job (3600 files, ~10 min) | `Manifest` rollup, one JLD2 read (< 10 ms) |
| 300 MB of per-item `println` logs | `EventLog` (JSONL), per-item `println` is not part of the API |
| Killed samples silently wedge the queue | Heartbeat + `is_stale` + `reclaim!` auto-recover on next run |
| Multiple masters double-execute the same key | `KeyLock` (`mkdir` advisory) + post-lock `is_done` re-check |
| Half-written JLD2 files after crash | `atomic_write` (tmp + fsync + rename) |
| Every project reinvents SLURM / Distributed bootstrap | `init_workers!(mode=:auto)` absorbs the pattern |

## Installation

This package depends on `ParamIO.jl` and `DataVault.jl`. During the
pre-registry phase, use `[sources]` with git URLs:

```toml
[deps]
DataVault       = "23f5f8f6-b4da-40ee-8c72-c53b6c5de94f"
ParallelManager = "be946ad2-3cb3-4b6e-8f7e-4a5ecc3c255b"
ParamIO         = "938a3ac2-d340-473c-bcf1-88af577e4ccf"

[sources]
ParamIO         = {url = "https://github.com/QAtlasHub/ParamIO.jl.git"}
DataVault       = {url = "https://github.com/QAtlasHub/DataVault.jl.git"}
ParallelManager = {url = "https://github.com/QAtlasHub/ParallelManager.jl.git"}
```

Then `julia --project -e 'using Pkg; Pkg.instantiate()'`.

## Tests

`Pkg.test()` runs ~4200 tests in ~22 seconds, including:

- `atomicio/` — atomic write, exception cleanup, concurrent writers
- `eventlog/` — JSON roundtrip, 50-task × 40-event concurrent write
- `manifest/` — save/load, `todo_keys`, 3600-key bench, corrupted file
- `keylock/` — basic mutex, heartbeat, stale reclaim, mutual exclusion under contention
- `init_workers/` — `:auto`, `:sequential`, `:threads`, `:slurm` env reading
- `run/` — minimal, manifest early-skip, 4-master keylock race, retry, gave_up

## See also

- [ParamIO.jl](https://github.com/QAtlasHub/ParamIO.jl) — config TOML parsing and `DataKey` enumeration
- [DataVault.jl](https://github.com/QAtlasHub/DataVault.jl) — `Vault` struct, atomic JLD2 save, `.done` markers
- [templateHPC.jl](https://github.com/sotashimozono/templateHPC.jl) — clone-to-start scaffold that wires all three together

## License

MIT. See [`LICENSE`](LICENSE).
