#==============================================================================
 examples/scripts/compute.jl — PHASE 1: the canonical 3-layer HPC driver.

 This is THE pattern to copy when you submit a parameter sweep to an HPC box.
 The whole stack is three layers, one line each:

   ParamIO    : config TOML        → Vector{DataKey}   (what to compute)
   DataVault  : (study, run)       → file storage      (where it goes)
   ParallelM. : run!(work_fn, …)   → parallel runtime  (do it, lock-safe, resumable)

 Run it (from the ParallelManager.jl package root):

     julia --project=. examples/scripts/compute.jl
     julia --project=. examples/scripts/compute.jl examples/configs/logistic.toml

 Run it AGAIN: the second run prints `:skip_complete` and exits in
 milliseconds — the manifest records what is already done, so a finished
 sweep is O(1) to re-check regardless of how many keys it has.
==============================================================================#

using ParamIO, DataVault, ParallelManager
include(joinpath(@__DIR__, "..", "src", "LogisticMap.jl"))
using .LogisticMap

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

#=  work_fn is a PURE function  (DataKey) -> Dict{String,Any}.
    Two things trip people (and LLMs) up here, so read carefully:

    1. work_fn does NOT call save!/mark_done!. It just RETURNS a Dict.
       The runtime calls DataVault.save!(vault, key, returned_dict) and writes
       the .done marker for you. If you grep compute.jl for `save!`, you won't
       find it — that is by design. (Returning a non-Dict is a runtime error.)

    2. Params are keyed by the DOTTED path  key.params["system.r"]  — NOT
       key.params["r"]. The leaf name alone raises KeyError. The dotted key
       matches the [paramsets.system] r=… block and the [datavault] path_keys.

    work_fn depends ONLY on `key` (no closure over master-side globals), so the
    exact same function is correct under :sequential, :threads and :slurm. =#
function work_fn(key::DataKey)
    r = Float64(key.params["system.r"])          # ← dotted key
    x0 = 0.1 + 0.13 * (key.sample - 1)            # per-sample initial condition
    return Dict{String,Any}(                       # ← RETURN the payload; do not save! it
        "r" => r,
        "x0" => x0,
        "lyapunov" => LogisticMap.lyapunov(r, x0),
        "tail" => LogisticMap.orbit_tail(r, x0),
    )
end

# Optional graceful-stop sentinel: the SLURM template (batch/submit_slurm.sh)
# traps SIGUSR1 ~60 s before the wall-clock kill and `touch`es this file; run!
# then stops dispatching new keys and returns cleanly. Unset locally ⇒ nothing.
const STOP_FLAG = get(ENV, "PM_STOP_FLAG", nothing)
opts = ParallelManager.RunOpts(; stop_flag=STOP_FLAG)

# ── Run — manifest-aware early-skip, multi-master lock safety (DataVault .running), retry. ──
# Returns a NamedTuple of counters: (stage, done, err, skipped, total, …).
result = ParallelManager.run!(work_fn, vault, keys; opts=opts)
@info "phase1 complete" result

# Reader-side convenience: ledger.csv, one row per completed key.
ledger = DataVault.build_ledger(vault)
@info "ledger written" ledger
