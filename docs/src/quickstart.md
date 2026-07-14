# Quick start

## Install

ParallelManager depends on `ParamIO` and `DataVault`. Until they land in
the registry, pin them via `[sources]`:

```toml
# your project's Project.toml
[deps]
DataVault       = "23f5f8f6-b4da-40ee-8c72-c53b6c5de94f"
ParallelManager = "be946ad2-3cb3-4b6e-8f7e-4a5ecc3c255b"
ParamIO         = "938a3ac2-d340-473c-bcf1-88af577e4ccf"

[sources]
ParamIO         = {url = "https://github.com/QAtlasHub/ParamIO.jl.git"}
DataVault       = {url = "https://github.com/QAtlasHub/DataVault.jl.git"}
ParallelManager = {url = "https://github.com/QAtlasHub/ParallelManager.jl.git"}
```

Then:

```bash
julia --project -e 'using Pkg; Pkg.instantiate()'
```

## Hello world

Create `config.toml`:

```toml
[study]
project_name  = "hello"
total_samples = 1
outdir        = "out"

[datavault]
path_keys = ["N", "J"]

[[paramsets]]
N = [4, 8, 16]
J = [0.5, 1.0]
```

And a script `run.jl`:

```julia
using ParamIO, DataVault, ParallelManager

spec  = ParamIO.load("config.toml")
keys  = ParamIO.expand(spec)
vault = DataVault.Vault("config.toml"; run="phase1")

ParallelManager.init_workers!(mode=:auto)

work_fn = key -> Dict{String,Any}(
    "N" => key.params["N"],
    "J" => key.params["J"],
    "energy" => key.params["N"] * key.params["J"],
)

result = ParallelManager.run!(work_fn, vault, keys)
@info "stage complete" result
```

First run processes all 6 keys and writes a JSONL event log. The second
run does nothing — it emits `:skip_complete` and exits in milliseconds:

```
┌ Info: stage complete
└   result = (stage = :phase1, done = 0, err = 0, skipped = 6, total = 6)
```

## Phase chaining without Stage/DAG

A dependent stage loads its parent's output inside the work function using
one line of `DataVault.load`. There is no path-building helper, no `Stage`
abstraction, no `DAG`.

```julia
phase1_vault = DataVault.Vault("config.toml"; run="phase1")
phase2_vault = DataVault.Vault("config.toml"; run="phase2")

work_fn = key -> begin
    phase1_payload = DataVault.load(phase1_vault, key)      # ← the one line
    energy = phase1_payload["N"] * phase1_payload["J"]^2
    return Dict{String,Any}("energy_squared" => energy)
end

ParallelManager.run!(work_fn, phase2_vault, keys)
```

This is the canonical replacement for `p2_phase1_mps_path`-style string
path builders that leak phase1's storage layout into phase2's code.

## Parallel execution

Want multi-threading inside one master?

```bash
julia --project --threads=8 run.jl
```

`init_workers!(mode=:auto)` detects `Threads.nthreads() > 1` and returns
`:threads`. For process-level parallelism, `run!` automatically fans out over
Distributed workers via `pmap` when `nprocs() > 1` (use
`init_workers!(mode=:distributed|:slurm)`); with only the master it runs
sequentially. Several independent masters can also share one vault:

```bash
# Master A
julia --project run.jl &

# Master B, same vault, same time — the .running lock arbitrates
julia --project run.jl &

wait
```

Both masters will hit the same vault, the per-key `.running` lock (DataVault's
`acquire_running!`) prevents double execution, and events from both processes
interleave safely in `out/events.jsonl`.

## SLURM

Use `templateHPC.jl`'s `batch/issp-example.sh` as the baseline. The Julia
side looks like:

```julia
ParallelManager.init_workers!(mode=:auto)   # detects SLURM_JOB_ID
```

and the bash side:

```bash
#SBATCH -N 1
#SBATCH -n 4            # master + 3 workers
#SBATCH -c 2

export JULIA_SLURM_N_WORKERS=$((SLURM_NTASKS - 1))
export JULIA_WORKER_CPUS=$SLURM_CPUS_PER_TASK

taskset -c 0 julia --project run.jl
```

`taskset -c 0` pins the master so `SlurmClusterManager`'s internal `srun`
can spawn workers across nodes without nested-job-step errors.
[`init_workers!`](@ref ParallelManager.init_workers!) handles the rest.
