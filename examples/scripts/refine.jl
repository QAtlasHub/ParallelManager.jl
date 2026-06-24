#==============================================================================
 examples/scripts/refine.jl — PHASE 2: phase chaining, the right way.

 A dependent phase reads its parent's output with ONE line of DataVault.load
 inside the work function. There is no Stage type, no DAG, no path-builder
 helper — you reuse the *same* DataKey against a different `run` vault.

 This is the canonical replacement for hand-rolled
 `p2_phase1_mps_path(key, …)` string builders that leak phase1's storage
 layout into phase2's code.

 Prerequisite: run compute.jl (phase1) first.

     julia --project=examples examples/scripts/compute.jl
     julia --project=examples examples/scripts/refine.jl
==============================================================================#

using ParamIO, DataVault, ParallelManager

const EXAMPLES = abspath(joinpath(@__DIR__, ".."))
const CONFIG = get(ARGS, 1, joinpath(EXAMPLES, "configs", "logistic.toml"))
const OUTDIR = get(ENV, "DATAVAULT_OUTDIR", joinpath(EXAMPLES, "out"))

spec = ParamIO.load(CONFIG)
keys = ParamIO.expand(spec)

# Same config, same DataKeys — only the `run` name differs. phase2 is a
# separate storage subtree + log.toml; the two never collide.
phase1 = DataVault.Vault(CONFIG; run="phase1", outdir=OUTDIR)
phase2 = DataVault.Vault(CONFIG; run="phase2", outdir=OUTDIR)

ParallelManager.init_workers!(; mode=:auto)

# Classify each point's dynamical regime from phase1's Lyapunov exponent.
function classify(key::DataKey)
    parent = DataVault.load(phase1, key)        # ← the one line: parent's Dict, by key
    λ = parent["lyapunov"]
    regime = λ > 1e-3 ? "chaotic" : (λ < -1e-2 ? "periodic" : "marginal")
    return Dict{String,Any}("r" => parent["r"], "lyapunov" => λ, "regime" => regime)
end

result = ParallelManager.run!(classify, phase2, keys)
@info "phase2 complete" result
