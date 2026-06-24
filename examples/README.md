# Examples — the ParamIO → DataVault → ParallelManager stack, end to end

A complete, **runnable, dependency-free** parameter sweep that uses all three
infra packages the way a real HPC job does. The "physics" is the logistic map
(`LogisticMap/`, a tiny pure-Julia package, no deps) so the whole thing runs in
seconds on any machine — but the *structure* is identical to a DMRG / Monte-Carlo
sweep on a cluster: a separate env that pulls the infra stack, and the work in
its own package handed to the workers with `run!(…; load=LogisticMap)`.

If you only read one thing in `infra/` to learn how to submit a computation,
read this directory.

## The three layers

| Layer | Package | Job | In this example |
| --- | --- | --- | --- |
| 1 | **ParamIO** | config TOML → `Vector{DataKey}` (*what* to compute) | `configs/logistic.toml` → 18 keys |
| 2 | **DataVault** | `(study, run)` → storage, `.done`, ledger (*where* it goes) | `out/data/logistic/phase1/…` |
| 3 | **ParallelManager** | `run!(work_fn, vault, keys)` → the runtime (*do it*, lock-safe, resumable) | `scripts/compute.jl` |

## Files

```
examples/
├── Project.toml              # the examples ENV — pulls the infra stack via [sources] + LogisticMap
├── configs/logistic.toml     # the sweep: r = [2.8 … 4.0], 3 samples ⇒ 18 DataKeys
├── LogisticMap/              # the work PACKAGE: pure-Julia kernel + work_fn(key), no deps
│   ├── Project.toml
│   └── src/LogisticMap.jl    #   lyapunov(r, x0), orbit_tail, and the pure work_fn
├── scripts/
│   ├── compute.jl            # PHASE 1 — the canonical driver (COPY THIS): run!(…; load=LogisticMap)
│   ├── refine.jl             # PHASE 2 — phase chaining via one DataVault.load line
│   └── summarize.jl          # the READER side — load data back, aggregate λ(r)
└── batch/submit_slurm.sh     # the HPC submission template (env vars + graceful stop)
```

## Run it

From the **ParallelManager.jl package root**, using the **examples env**
(`examples/Project.toml`, which pulls the three infra packages + the `LogisticMap`
work package — `--project=examples`):

```bash
julia --project=examples examples/scripts/compute.jl     # phase 1: computes 18 keys
julia --project=examples examples/scripts/compute.jl     # run AGAIN → instant :skip_complete
julia --project=examples examples/scripts/refine.jl      # phase 2: reads phase1, classifies regime
julia --project=examples examples/scripts/summarize.jl   # reader: λ(r) table
```

(Output goes to `examples/out/`, which is gitignored — delete it to start fresh.)

### What you should see

Phase 1, first run computes everything; **run it again and it does nothing in O(1)**
— the manifest already knows all 18 keys are done:

```
# run 1
result = (stage = :phase1, done = 18, err = 0, skipped = 0,  total = 18)
# run 2
result = (stage = :phase1, done = 0,  err = 0, skipped = 18, total = 18)   # ← :skip_complete
```

The reader aggregates the Lyapunov exponent over `r`. Note the physics checks out:
λ < 0 (periodic) below the chaos threshold, λ ≈ 0 (marginal) right at r = 3.5699,
and **λ = +0.6931 = ln 2 at r = 4.0 — the exact textbook value** for the fully
chaotic logistic map:

```
  r        mean λ      regime
  ─────────────────────────────────
  2.8000    -0.2231   periodic
  3.2000    -0.9163   periodic
  3.5000    -0.8725   periodic
  3.5699    -0.0038   marginal
  3.8300    -0.3695   periodic
  4.0000    +0.6931   chaotic
```

On disk, DataVault lays out one directory per parameter point (named from
`path_keys`), with a frozen discovery anchor under `.datavault/`:

```
out/
├── .datavault/logistic/
│   ├── phase1.log.toml          # discovery anchor — never moves, lets analysis
│   └── phase2.log.toml          #   code find the data without knowing the config path
└── data/logistic/phase1/
    ├── config_snapshot.toml     # the exact config used, frozen
    ├── ledger.csv               # one row per done key (params, git_hash, timestamp)
    └── sysr2.80/data_sample001.jld2   # "sysr2.80" = path_keys ["system.r"], r = 2.80
```

## The six things that confuse people (and LLMs) — answered here

1. **`work_fn` RETURNS a `Dict`; it does not `save!`.** Grep `compute.jl` for
   `save!` — it's not there. `run!` calls `DataVault.save!(vault, key, returned_dict)`
   and writes the `.done` marker for you. Returning a non-`Dict` is a runtime error.
   See `scripts/compute.jl`, `work_fn`.

2. **Params use the DOTTED key `key.params["system.r"]`, never `["r"]`.**
   ParamIO flattens `[paramsets.system] r=…` to the dotted key `"system.r"`. The
   plain leaf name raises `KeyError`. The same dotted form appears in
   `[datavault] path_keys`.

3. **One config file, parsed by every layer.** `ParamIO.load(CONFIG)` and
   `DataVault.Vault(CONFIG)` each parse it independently — you do **not** thread a
   single parsed object through. Harmless, just surprising.

4. **Phases don't need a DAG.** `refine.jl` (phase 2) reads phase 1 with a single
   `DataVault.load(phase1_vault, key)` inside its `work_fn` — same `DataKey`,
   different `run`. No `Stage`, no path-builder, no layout leakage.

5. **The same `compute.jl` scales without edits.** `init_workers!(mode=:auto)`
   returns `:sequential` / `:threads` / `:slurm` from the environment; `run!`
   fans out over `Distributed` workers when they exist and runs sequentially
   otherwise. To go to a cluster you change the *batch script*, not the Julia.

6. **Your work reaches the workers because it's a package + `load=`.**
   `init_workers!` spawns workers with `--project` but loads no packages. Put the
   work in a package (here `LogisticMap`) and pass `run!(…; load=LogisticMap)`:
   the runtime `using`s it — plus `ParamIO`/`DataVault`/`ParallelManager` — in
   `Main` on every worker before fan-out. No `@everywhere`, no hand-rolled
   `remotecall` broadcast, nothing to forget. The failure that used to read
   `KeyError: <Module> not found` on a worker (only ever on a real cluster) is
   gone by construction.

## Going to a cluster

`batch/submit_slurm.sh` is the bash half: it allocates nodes, exports the env
vars `init_workers!(mode=:slurm)` reads (`JULIA_SLURM_N_WORKERS`,
`JULIA_WORKER_CPUS`, `DATAVAULT_OUTDIR`, …), and traps `SIGUSR1` to `touch` the
stop-flag that `compute.jl` passes to `RunOpts(stop_flag=…)` for graceful
shutdown. The Julia side is unchanged.

```bash
sbatch examples/batch/submit_slurm.sh examples/configs/logistic.toml
```

For a long production sweep, swap `run!` for `run_loop!` in `compute.jl`: it
keeps re-scanning for work (picking up keys freed by crashed siblings) until
the sweep is fully done or the stop-flag is raised.

## Real-world reference

For a full physics application built on this exact stack, see
`apps/ReducedEnvExperiments.jl/projects/*/scripts/compute.jl` — same shape,
real DMRG `work_fn`, real SLURM batch scripts in `batch/`.
