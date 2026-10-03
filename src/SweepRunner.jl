"""
    SweepRunner

HPC experiment runtime for Julia.

Wraps `ParamIO.jl` and `DataVault.jl` with a unified `run!` that handles
parallel dispatch, advisory locking, `.done` rollups, structured event
logging, and retry. Designed to replace the recurring "glue" layer that
every HPC research project re-invents.

# Modules

Each file is one concern, one module-scope piece. They can be used
independently.

| File            | Public API                                              |
| :-------------- | :------------------------------------------------------ |
| `AtomicIO.jl`   | [`atomic_write`](@ref), [`atomic_touch`](@ref)          |
| `EventLog.jl`   | [`EventLog`](@ref), [`log_event`](@ref)                 |
| `Manifest.jl`   | [`Manifest`](@ref), [`load_manifest`](@ref), [`save_manifest`](@ref), [`add_complete!`](@ref), [`is_complete`](@ref), [`todo_keys`](@ref), [`manifest_path`](@ref) |
| `InitWorkers.jl`| [`init_workers!`](@ref), [`detect_mode`](@ref)          |
| `Run.jl`        | [`run!`](@ref), [`RunOpts`](@ref), [`manifest_root`](@ref) |
| `TaskTable.jl`  | [`TaskTable`](@ref), [`next_task!`](@ref), [`settle!`](@ref), [`task_counts`](@ref) |
| `Progress.jl`   | [`report_progress`](@ref), [`resume_point`](@ref)       |
| `Status.jl`     | [`read_status`](@ref), [`print_status`](@ref), [`note_workers!`](@ref) |
| `Locks.jl`      | [`locks`](@ref), [`judge_lock`](@ref), [`reap_dead_locks!`](@ref) |
| `Control.jl`    | [`control!`](@ref), [`should_stop`](@ref), [`stop_point`](@ref) |
| `Pool.jl`       | [`SizedPool`](@ref), [`KeyReq`](@ref), [`LocalSpawner`](@ref), [`SlurmStepSpawner`](@ref) |
| `Checkpoint.jl` | [`save_checkpoint!`](@ref), [`load_checkpoint`](@ref), [`checkpoint_due`](@ref), [`check_checkpoints`](@ref) |
| `Account.jl`    | [`account_snapshot`](@ref), [`print_account`](@ref) |
| `Cost.jl`       | [`key_costs`](@ref), [`cost_summary`](@ref), [`measured_cost`](@ref), [`note_key!`](@ref) |
| `Campaign.jl`   | [`load_campaign`](@ref), [`plan_campaign`](@ref), [`run_campaign!`](@ref) |
| `Jobs.jl`       | [`Scheduler`](@ref), [`JobPolicy`](@ref), [`decide`](@ref), [`manage!`](@ref) |

# Quick start

```julia
using ParamIO, DataVault, SweepRunner

spec  = ParamIO.load("config.toml")
keys  = ParamIO.expand(spec)
vault = DataVault.Vault("config.toml"; run="phase1")

SweepRunner.init_workers!(mode=:auto)
work_fn = key -> Dict{String,Any}("x" => compute(key))
SweepRunner.run!(work_fn, vault, keys)
```

# Design constraints

1. `work_fn` is a **pure function** — no IO, no globals, no logging. All of
   that lives in the runtime.
2. There is **no per-item `println` API**. Use [`log_event`](@ref) for
   structured events. Per-item prints are the reason `FiniteTemperature.jl`
   used to generate 300 MB job logs.
3. The `Manifest` is **monotonic** — keys are only added, never removed.
   Re-computing the same root is considered a contract violation; branch
   into a new `outdir` instead.
4. Multi-master coordination is entirely delegated to
   `DataVault.acquire_running!`, which uses POSIX `link()` for
   atomic "create iff not exists" on NFS.  No `flock`, no central
   service, no separate `locks/` tree — `.running` itself is the lock.

# See also

- `ParamIO.canonical` — the stable string form used by [`Manifest`](@ref)
  as a directory-safe key identity.
- `DataVault.Vault`, `DataVault.save!`, `DataVault.is_done`,
  `DataVault.acquire_running!` — the storage + lock layer [`run!`](@ref)
  delegates to.
"""
module SweepRunner

# Do NOT add a per-item `println` API anywhere in this module. Structured
# events go through EventLog (JSONL) only. This is a structural answer to
# the痛点 of per-item println noise.

include("AtomicIO.jl")
include("EventLog.jl")
include("Manifest.jl")
include("InitWorkers.jl")
include("Liveness.jl")
include("TaskTable.jl")       # the master's table of a round's units, and its queue
include("Account.jl")         # where a job's core-hours went
include("Master.jl")          # a master's identity; state_root
include("Locks.jl")           # judge_lock: ask the holder's master; locks(), reap_dead_locks!
include("Run.jl")
include("Control.jl")         # control!: requests to a running master; should_stop / stop_point
include("Pool.jl")            # SizedPool: workers sized to the keys they run
include("Progress.jl")        # report_progress / resume_point, the context work_fn runs in
include("Checkpoint.jl")      # save_checkpoint! / load_checkpoint / checkpoint_due inside work_fn
include("Status.jl")          # the status file a master rewrites, and reading it from outside
include("Cost.jl")            # what a key cost: key_costs, cost_summary, measured_cost
include("Observe.jl")         # one source observation per process per run!, for each .done
include("Artifacts.jl")        # artifact_affinity; ArtifactBusy deferral lives in Run.jl
include("Prerequisite.jl")
include("Preflight.jl")
include("Campaign.jl")        # a meta config: which stages run, in what order, under which filters
include("Jobs.jl")            # Scheduler, JobPolicy, Ledger: deciding submissions from what is left
include("CLI.jl")             # `sweeprunner status …`

end # module SweepRunner
