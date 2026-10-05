# SweepRunner.jl

**HPC experiment runtime for Julia** — multi-master safe, crash-recoverable,
`Pkg.test()`-fast.

Wraps [ParamIO.jl](https://github.com/QAtlasHub/ParamIO.jl) and
[DataVault.jl](https://github.com/QAtlasHub/DataVault.jl) with a unified
[`run!`](@ref SweepRunner.run!) that handles parallel dispatch,
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
  up where it left off: locks whose holder is gone are removed; one nobody answers for is reclaimed after
  `stale_after`; one a reporting master lists as held is left alone.
- **Early skip** — full-done re-runs take O(1) filesystem operations
  (a single `manifest.jld2` read), not O(N) per-key `.done` stats.
  The test suite bounds the manifest read plus `todo_keys` for 3600 keys at 500 ms, and a
  3600-key warm `run!` at 1 s.
- **Structured events** — JSONL event log atomic across concurrent writers;
  per-item `println` is a non-goal, by design.
- **One entry point for all parallel modes** —
  [`init_workers!(mode=:auto)`](@ref SweepRunner.init_workers!)
  sets up `:sequential` / `:threads` / `:distributed` / `:slurm` (keys are dispatched over
  processes: under `:threads` they run on the master one at a time, with threads inside a key)
  depending on environment.
- **Pure work functions** — your physics is a plain
  `(DataKey) -> Dict`, IO/locking/logging live in the runtime.

## 30-second tour

```julia
using ParamIO, DataVault, SweepRunner

spec  = ParamIO.load("config.toml")
keys  = ParamIO.expand(spec)
vault = DataVault.Vault("config.toml"; run="phase1")

SweepRunner.init_workers!(mode=:auto)
work_fn = key -> Dict{String,Any}("x" => compute(key))
SweepRunner.run!(work_fn, vault, keys)
```

Re-running the same script after completion emits `:skip_complete` and
returns after one manifest read, whatever `length(keys)` is.

## Pain points it answers

| Pain | Answer |
| :--- | :--- |
| `.done` files rescanned every job (one stat per key, on a network file system) | [`Manifest`](@ref SweepRunner.Manifest) rollup, one JLD2 read |
| 300 MB of per-item `println` logs | [`EventLog`](@ref SweepRunner.EventLog) (JSONL); per-item `println` is not part of the API |
| Killed samples silently wedge the queue | Heartbeat + stale-lock reclaim (DataVault `.running`) auto-recover |
| Multiple masters double-execute the same key | Per-key `.running` advisory lock (`acquire_running!`) + post-lock `is_done` re-check |
| Half-written JLD2 files after crash | [`atomic_write`](@ref SweepRunner.atomic_write) (tmp + fsync + rename) |
| Every project reinvents SLURM / Distributed bootstrap | [`init_workers!`](@ref SweepRunner.init_workers!)`(mode=:auto)` |

## See also

- [ParamIO.jl](https://github.com/QAtlasHub/ParamIO.jl) — config TOML parsing and `DataKey` enumeration
- [DataVault.jl](https://github.com/QAtlasHub/DataVault.jl) — `Vault` struct, atomic JLD2 save, `.done` markers
- [templateHPC.jl](https://github.com/sotashimozono/templateHPC.jl) — clone-to-start scaffold that wires all three together
