# ParallelManager.jl

**HPC experiment runtime for Julia** — multi-master safe, crash-recoverable,
`Pkg.test()`-fast.

Wraps [ParamIO.jl](https://github.com/QAtlasHub/ParamIO.jl) and
[DataVault.jl](https://github.com/QAtlasHub/DataVault.jl) with a unified
[`run!`](@ref ParallelManager.run!) that handles parallel dispatch,
advisory locking, `.done` rollups, structured event logging, and retry.

## Pages

```@contents
Pages = ["quickstart.md", "architecture.md", "api.md", "guides.md"]
Depth = 2
```

## Highlights

- **Multi-master safe** — several `julia` processes can hit the same vault
  root without double-executing any key (DataVault `.running` advisory lock).
- **Crash recovery** — `kill -9` a master mid-run and the next `run!` picks
  up where it left off via heartbeat-based stale lock reclaim.
- **Early skip** — full-done re-runs take O(1) filesystem operations
  (a single `manifest.jld2` read), not O(N) per-key `.done` stats.
  3600-key warm re-run ≈ **3.5 ms**.
- **Structured events** — JSONL event log atomic across concurrent writers;
  per-item `println` is a non-goal, by design.
- **One entry point for all parallel modes** —
  [`init_workers!(mode=:auto)`](@ref ParallelManager.init_workers!)
  dispatches to `:sequential` / `:threads` / `:distributed` / `:slurm`
  depending on environment.
- **Pure work functions** — your physics is a plain
  `(DataKey) -> Dict`, IO/locking/logging live in the runtime.

## 30-second tour

```julia
using ParamIO, DataVault, ParallelManager

spec  = ParamIO.load("config.toml")
keys  = ParamIO.expand(spec)
vault = DataVault.Vault("config.toml"; run="phase1")

ParallelManager.init_workers!(mode=:auto)
work_fn = key -> Dict{String,Any}("x" => compute(key))
ParallelManager.run!(work_fn, vault, keys)
```

Re-running the same script after completion emits `:skip_complete` and
exits in milliseconds regardless of `length(keys)`.

## Pain points it answers

| Pain | Answer |
| :--- | :--- |
| `.done` files rescanned every job (3600 files, ~10 min) | [`Manifest`](@ref ParallelManager.Manifest) rollup, one JLD2 read |
| 300 MB of per-item `println` logs | [`EventLog`](@ref ParallelManager.EventLog) (JSONL); per-item `println` is not part of the API |
| Killed samples silently wedge the queue | Heartbeat + stale-lock reclaim (DataVault `.running`) auto-recover |
| Multiple masters double-execute the same key | Per-key `.running` advisory lock (`acquire_running!`) + post-lock `is_done` re-check |
| Half-written JLD2 files after crash | [`atomic_write`](@ref ParallelManager.atomic_write) (tmp + fsync + rename) |
| Every project reinvents SLURM / Distributed bootstrap | [`init_workers!`](@ref ParallelManager.init_workers!)`(mode=:auto)` |

## See also

- [ParamIO.jl](https://github.com/QAtlasHub/ParamIO.jl) — config TOML parsing and `DataKey` enumeration
- [DataVault.jl](https://github.com/QAtlasHub/DataVault.jl) — `Vault` struct, atomic JLD2 save, `.done` markers
- [templateHPC.jl](https://github.com/sotashimozono/templateHPC.jl) — clone-to-start scaffold that wires all three together
