#==============================================================================
 examples/scripts/compute.jl — PHASE 1: the canonical 3-layer HPC driver.

 This is THE pattern to copy when you submit a parameter sweep to an HPC box.
 The whole stack is three layers, one line each:

   ParamIO    : config TOML        → Vector{DataKey}   (what to compute)
   DataVault  : (study, run)       → file storage      (where it goes)
   ParallelM. : run!(work_fn, …)   → parallel runtime  (do it, lock-safe, resumable)

 The work itself (LogisticMap.work_fn) lives in a PACKAGE, not in this script.
 That is the whole point: you hand the work module to the workers with
 `run!(…; load=LogisticMap)` and the runtime `using`s it (plus the seam
 packages) on every worker for you. No `@everywhere`, no hand-rolled
 `for w in workers(); remotecall_fetch(…, :(using …)); end` broadcast — and
 nothing to forget (the broadcast is where the classic
 `UndefVarError: <X> not defined on a worker` footgun lives).

 Run it (from the ParallelManager.jl package root, with the EXAMPLES env):

     julia --project=examples examples/scripts/compute.jl
     julia --project=examples examples/scripts/compute.jl examples/configs/logistic.toml

 Run it AGAIN: the second run prints `:skip_complete` and exits in
 milliseconds — the manifest records what is already done, so a finished
 sweep is O(1) to re-check regardless of how many keys it has.
==============================================================================#

using ParamIO, DataVault, ParallelManager
using LogisticMap   # the work PACKAGE (defines the pure work_fn); shipped to workers via load=

const EXAMPLES = abspath(joinpath(@__DIR__, ".."))
const CONFIG = get(ARGS, 1, joinpath(EXAMPLES, "configs", "logistic.toml"))
# outdir precedence (DataVault resolves it): kwarg > ENV["DATAVAULT_OUTDIR"] > config.
const OUTDIR = get(ENV, "DATAVAULT_OUTDIR", joinpath(EXAMPLES, "out"))

# ── Layer 1 · ParamIO — parse the config and expand the sweep into DataKeys ──
spec = ParamIO.load(CONFIG)
keys = ParamIO.expand(spec)          # Vector{DataKey}, one per (r × sample) = 18

# ── Layer 2 · DataVault — attach to a (study, run). Writes the discovery
#    anchor out/.datavault/logistic/phase1.log.toml and a config snapshot. ──
vault = DataVault.Vault(CONFIG; run="phase1", outdir=OUTDIR)

# ── Layer 3 · ParallelManager — bootstrap workers. mode=:auto picks
#    :sequential locally, :threads if -t>1, :slurm inside a SLURM job. ──
ParallelManager.init_workers!(; mode=:auto)

#=  The work is LogisticMap.work_fn — a PURE (DataKey) -> Dict{String,Any}.
    Two things trip people (and LLMs) up, both handled by structure here:

    1. work_fn does NOT call save!/mark_done!. It just RETURNS a Dict; the
       runtime calls DataVault.save!(vault, key, returned_dict) and writes the
       .done marker. (Returning a non-Dict is a runtime error.)

    2. It must run on the WORKERS. Because the work lives in a package, we pass
       `load=LogisticMap` below and run! loads it (and ParamIO/DataVault/
       ParallelManager) in Main on every worker before fan-out. Put your work in
       a package and this whole class of "module not defined on a worker" bugs
       disappears. =#

# Optional graceful-stop sentinel: the SLURM template (batch/submit_slurm.sh)
# traps SIGUSR1 ~60 s before the wall-clock kill and `touch`es this file; run!
# then stops dispatching new keys and returns cleanly. Unset locally ⇒ nothing.
const STOP_FLAG = get(ENV, "PM_STOP_FLAG", nothing)
opts = ParallelManager.RunOpts(; stop_flag=STOP_FLAG)

# ── Run — manifest-aware early-skip, multi-master lock safety (DataVault .running), retry.
#    `load=LogisticMap` ships the work module to the workers. Returns a NamedTuple
#    of counters: (stage, done, err, skipped, total, …). ──
result = ParallelManager.run!(LogisticMap.work_fn, vault, keys; opts=opts, load=LogisticMap)
@info "phase1 complete" result

# Reader-side convenience: ledger.csv, one row per completed key.
ledger = DataVault.build_ledger(vault)
@info "ledger written" ledger
