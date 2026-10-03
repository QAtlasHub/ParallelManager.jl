# Preflight — answer "can this campaign run" before spending the compute.
#
# Ported out of FiniteTemperature.jl, where it was written after the same class
# of defect had cost a campaign four separate times. The shape is always the
# same: **declared in one place, relied on in another, checked nowhere.**
#
#   [datavault] float_format = "auto"   the config asked; the path builder ignored it
#   beta_quench ⊆ beta_targets          required in a comment
#   the downstream stage knowing the    required in a comment
#     upstream discretisation
#   the checkpoint directory carrying   not carried at all
#     the parameter that distinguishes runs
#
# Each is decidable by comparing strings before any physics runs. What lives
# here is the part that is not specific to any one study:
#
#   * the report — `Finding`, `PreflightReport`, `launchable`, and a `show` that
#     groups by layer so a refusal names its own cause
#   * `check_injective!` — distinct parameter points must get distinct paths.
#     This is THE check: a collision means one point silently overwrites another
#     AND its `.done` marker completes work that never ran.
#   * `check_opens!` — opening a vault can itself fail (a shared `project_name`
#     with differing `path_keys` is a startup crash, not a warning), and that
#     belongs in the report rather than as a stack trace out of the checker
#   * `on_grid` / `representative_keys` — the two predicates every study repeats
#
# A study adds its own layers on top: which values must lie on which grid, and
# whether the artefact a later stage LOOKS for is the one an earlier stage
# WRITES. That last one is the decisive check and it cannot be generic, because
# only the study knows what its stages hand each other — but it is always built
# the same way: generate the path from BOTH sides and compare the strings,
# rather than trusting a comment that says they agree.

using Printf
using ParamIO: DataKey
using DataVault: DataVault

export Finding, PreflightReport, launchable, n_errors, n_warns
export check_injective!, check_opens!, on_grid, representative_keys

"""
    Finding(layer, severity, where, message)

One reason a campaign cannot (or should not) start.

`layer` is the study's own vocabulary — `:config`, `:grid`, `:injective`,
`:crossphase` in the original — and exists so that a refusal is attributable.
Layering that cannot distinguish two different causes is decoration; a study's
tests should assert that a deliberately broken config is rejected by the RIGHT
layer, not merely rejected.
"""
struct Finding
    layer::Symbol
    severity::Symbol        # :error / :warn
    where::String
    message::String
end

"""
    PreflightReport(findings) -> PreflightReport

Everything preflight found, in the order the layers were run.

Carries `findings::Vector{Finding}` and `layers`, the display order. `isempty`
means the campaign is clean; [`launchable`](@ref) is the gate to branch on,
because a report can be non-empty and still launchable when every finding is a
warning.
"""
struct PreflightReport
    findings::Vector{Finding}
    layers::Vector{Symbol}  # display order
end

PreflightReport(fs::Vector{Finding}) = PreflightReport(fs, unique(f.layer for f in fs))

Base.isempty(r::PreflightReport) = isempty(r.findings)
"""
    n_errors(report::PreflightReport) -> Int

How many of the report's findings are errors: with any, [`launchable`](@ref) is `false`.
"""
n_errors(r::PreflightReport) = count(f -> f.severity === :error, r.findings)

"""
    n_warns(report::PreflightReport) -> Int

How many of the report's findings are warnings: shown, and they do not refuse.
"""
n_warns(r::PreflightReport) = count(f -> f.severity === :warn, r.findings)

"""
    launchable(r) -> Bool

True when nothing at `:error` severity was found. Warnings do not block.

Callers should gate on this and **exit non-zero** rather than printing a verdict
for a human to grep: a grep over output reports a parse error as success.
"""
launchable(r::PreflightReport) = n_errors(r) == 0

function Base.show(io::IO, r::PreflightReport)
    if isempty(r)
        println(io, "✅ preflight: nothing to report")
        return nothing
    end
    for layer in r.layers
        fs = filter(f -> f.layer === layer, r.findings)
        isempty(fs) && continue
        println(io, "\n── ", layer, " ──")
        for f in fs
            println(
                io, "  ", f.severity === :error ? "✗" : "!", " [", f.where, "] ", f.message
            )
        end
    end
    @printf(
        io,
        "\n%s  errors=%d warnings=%d\n",
        launchable(r) ? "✅ launchable" : "❌ NOT launchable",
        n_errors(r),
        n_warns(r)
    )
    return nothing
end

# ── predicates every study repeats ──────────────────────────────────────────

"""
    on_grid(x, step; atol=1e-9) -> Bool

Is `x` a multiple of `step`?

A value that is not is one the evolution steps *past*: the artefact named after
it is never written, and the failure surfaces much later as a missing file in a
downstream stage, long after the compute is spent.
"""
function on_grid(x::Real, step::Real; atol::Real=1e-9)
    return isapprox(x / step, round(x / step); atol=atol)
end

"""
    representative_keys(keys) -> Vector{DataKey}

One key per distinct parameter point.

`ParamIO.expand` enumerates (point × sample), but a path is a property of the
point — the sample only varies the filename. Counting collisions over all keys
would therefore report `n_samples` false collisions for every real one.
"""
function representative_keys(keys)
    seen = Set{Any}()
    out = eltype(keys)[]
    for k in keys
        k.params in seen && continue
        push!(seen, k.params)
        push!(out, k)
    end
    return out
end

# ── the generic checks ──────────────────────────────────────────────────────

"""
    check_injective!(findings, layer, label, name, paths)

Assert that `paths` has no duplicates, i.e. that distinct parameter points map
to distinct files.

This is the check worth having. Under a content-blind float rendering (`%.2f`),
`h = 0.002` and `0.004` both render `h0.00`, so two points share a directory —
and not only their observables. They share the **status marker**, which means
the second point is reported complete without ever running. A sweep of 168
points then finishes as 132 with no error anywhere.

Pass the status paths as well as the data paths. Checking only the data half
catches the overwrite and misses the skipped work, which is the worse of the two.
"""
function check_injective!(
    findings::Vector{Finding},
    layer::Symbol,
    label::AbstractString,
    name::AbstractString,
    paths::AbstractVector{<:AbstractString},
)
    n, u = length(paths), length(Set(paths))
    if u < n
        dupes = [p for p in Set(paths) if count(==(p), paths) > 1]
        push!(
            findings,
            Finding(
                layer,
                :error,
                String(label),
                "$name: $n parameter points → $u distinct paths. " *
                "$(n - u) point(s) share a path with another, so one silently overwrites " *
                "the other and its status marker completes work that never ran. " *
                "First collision: $(first(sort(collect(dupes))))",
            ),
        )
    end
    return findings
end

"""
    check_opens!(findings, layer, label, open_fn) -> Union{Any,Nothing}

Run `open_fn()` and, if it throws, record the failure instead of propagating it.

Opening a vault is itself a thing that fails — most commonly a DataVault
`log.toml conflict` when two stages of one campaign share a `project_name` but
declare different `path_keys`. A driver that opens several vaults in a row dies
at startup on that, so it belongs in the report next to everything else rather
than as a stack trace out of the checker.
"""
function check_opens!(
    findings::Vector{Finding}, layer::Symbol, label::AbstractString, open_fn::Function
)
    try
        return open_fn()
    catch e
        push!(
            findings,
            Finding(
                layer,
                :error,
                String(label),
                "could not open the vault: $(sprint(showerror, e))",
            ),
        )
        return nothing
    end
end
