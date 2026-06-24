#==============================================================================
 examples/scripts/summarize.jl — the READER side.

 Reads the data back with DataVault.load and aggregates λ(r). The point to
 notice: compute.jl never called save! — yet the data is here. The runtime
 persisted every work_fn return value for us.

     julia --project=examples examples/scripts/summarize.jl
==============================================================================#

using ParamIO, DataVault, Printf, Statistics

const EXAMPLES = abspath(joinpath(@__DIR__, ".."))
const CONFIG = get(ARGS, 1, joinpath(EXAMPLES, "configs", "logistic.toml"))
const OUTDIR = get(ENV, "DATAVAULT_OUTDIR", joinpath(EXAMPLES, "out"))

vault = DataVault.Vault(CONFIG; run="phase1", outdir=OUTDIR)

# Collect λ samples grouped by r over all completed keys.
by_r = Dict{Float64,Vector{Float64}}()
for key in DataVault.keys(vault; status=:done)
    d = DataVault.load(vault, key)
    push!(get!(by_r, d["r"], Float64[]), d["lyapunov"])
end

println("\n  r        mean λ      regime")
println("  ─────────────────────────────────")
for r in sort(collect(keys(by_r)))
    λ = mean(by_r[r])
    regime = λ > 1e-3 ? "chaotic" : (λ < -1e-2 ? "periodic" : "marginal")
    @printf("  %-7.4f  %+8.4f   %s\n", r, λ, regime)
end
println()
