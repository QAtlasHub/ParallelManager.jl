# Cost — what a key cost, measured, and kept where the next job can read it.
#
# The event log said a key was acquired and done, and not what it took. After 20 000 keys of a
# campaign there was no table of how long a key of a given size ran, how many cores it used or
# how much memory it peaked at, so "will it finish", "what fits a 30-minute job" and "how much
# memory to ask for" were a hand-fitted formula and a guess.
#
# `key_done` now carries wall time, CPU time, cores, peak RSS, the node, the key's class and
# whatever the application adds (`note_key!`). `key_costs` reads those records back, and
# `write_cost_table` keeps a per-class summary under the sweep's state directory:
#
#     <state_root>/costs.json
#
# `measured_cost` / `measured_mem` are hooks built from that table: they answer from the
# measurement, and from the application's own estimate for a class not seen yet.

using JSON3
using DataVault: Vault
using ParamIO: DataKey

# ── measuring one key ───────────────────────────────────────────────────────────────────────────

# Start the peak-memory count again, so a key's peak is its own and not the largest key this
# worker has ever run. Linux only (`/proc/self/clear_refs`). Returns whether it worked: where it
# did not, the peak read afterwards is the process's, and the record says so.
function _reset_peak_rss()::Bool
    Sys.islinux() || return false
    try
        write("/proc/self/clear_refs", "5")
        return true
    catch
        return false
    end
end

# Peak resident memory in bytes since the last reset: `VmHWM` on Linux, else the process's
# maximum.
function _peak_rss()::Int
    try
        if Sys.islinux()
            for line in eachline("/proc/self/status")
                startswith(line, "VmHWM:") || continue
                return parse(Int, split(line)[2]) * 1024
            end
        end
    catch
    end
    return Int(Sys.maxrss())
end

"""
    note_key!(; kwargs...) -> Bool

Add fields to the `key_done` record of the key this `work_fn` call is on — a size label, how many
segments ran, anything worth having beside the timing. They appear under `note` in the event and
in [`key_costs`](@ref). Returns `false` and does nothing outside a `run!`.

```julia
SweepRunner.note_key!(segments = nseg, bond_dim = chi)
```
"""
function note_key!(; kwargs...)
    ctx = _KEY[]
    ctx === nothing && return false
    for (k, v) in kwargs
        ctx.notes[String(k)] = v
    end
    return true
end

# ── reading the records back ────────────────────────────────────────────────────────────────────

"""
    KeyCost

One attempt at a key, as measured: `stage`, `key` (canonical), `class` (from `run!`'s `key_class`,
`""` when none was given), `wall` and `cpu` in seconds, `cores` the worker had, `rss` peak
resident bytes, `host`, the `attempt` number, `note` (what the application added with
[`note_key!`](@ref)), the attempt's `outcome` (`"ok"`, or `"error"`, `"stopped"`, `"lock_lost"`,
`"worker_died"` for one that did not finish the key), and `rss_scope`: `"key"` when the peak is
this attempt's own, `"process"` when it is the worker's since it started (the reset is not
available there), `""` when no peak was read.

`cpu / (wall * cores)` is how much of its cores the attempt used.
"""
struct KeyCost
    stage::String
    key::String
    class::String
    wall::Float64
    cpu::Float64
    cores::Int
    rss::Int
    host::String
    attempt::Int
    note::Dict{String,Any}
    outcome::String
    rss_scope::String
end

# A finished attempt whose peak is its own: what a record is unless it says otherwise.
function KeyCost(stage, key, class, wall, cpu, cores, rss, host, attempt, note)
    return KeyCost(
        stage, key, class, wall, cpu, cores, rss, host, attempt, note, "ok", "key"
    )
end

function _event_files(outdir::AbstractString)
    isdir(outdir) || return String[]
    return [
        joinpath(outdir, f) for f in readdir(outdir) if
        startswith(f, "events_") && endswith(f, ".jsonl") && f != "events_merged.jsonl"
    ]
end

"""
    key_costs(vault) -> Vector{KeyCost}
    key_costs(outdir::AbstractString; stage=nothing) -> Vector{KeyCost}

Every attempt on record in the event logs under a vault's outdir — `key_done` (the attempt that
finished a key) and `key_spent` (one that did not: failed, stopped, lost its lock, or whose
worker died) — for the vault's stage, or for `stage` (all stages when `nothing`). Records written
before the measurement existed carry only the wall time and are returned with `cpu = NaN`,
`cores = 0`, `rss = 0`.
"""
function key_costs(outdir::AbstractString; stage=nothing)
    out = KeyCost[]
    want = stage === nothing ? nothing : String(stage)
    for f in _event_files(outdir)
        for line in eachline(f)
            # A cheap test first: most lines of a log are neither.
            (occursin("\"key_done\"", line) || occursin("\"key_spent\"", line)) || continue
            e = try
                JSON3.read(line, Dict{String,Any})
            catch
                continue
            end
            kind = get(e, "kind", "")
            (kind == "key_done" || kind == "key_spent") || continue
            st = String(get(e, "stage", ""))
            (want === nothing || st == want) || continue
            note = get(e, "note", nothing)
            push!(
                out,
                KeyCost(
                    st,
                    String(get(e, "key", "")),
                    String(something(get(e, "class", ""), "")),
                    Float64(get(e, "secs", NaN)),
                    Float64(something(get(e, "cpu", NaN), NaN)),
                    Int(get(e, "cores", 0)),
                    Int(get(e, "rss", 0)),
                    String(get(e, "host", "")),
                    Int(get(e, "attempt", 1)),
                    note isa AbstractDict ? Dict{String,Any}(note) : Dict{String,Any}(),
                    kind == "key_done" ? "ok" : String(get(e, "outcome", "?")),
                    String(get(e, "rss_scope", haskey(e, "rss") ? "key" : "")),
                ),
            )
        end
    end
    return out
end

key_costs(vault::Vault) = key_costs(vault.outdir; stage=vault.run)

# The q-quantile of a sorted, non-empty vector (nearest rank).
function _quantile(sorted::AbstractVector{<:Real}, q::Real)
    return sorted[clamp(ceil(Int, q * length(sorted)), 1, length(sorted))]
end

"""
    cost_summary(costs; by = c -> c.class) -> Dict{String,NamedTuple}

Per class: `(; n, wall_median, wall_p90, cpu_median, cores, rss_peak, efficiency, unfinished,
rss_process)`.

A key's time is the SUM over its attempts — the ones that failed, were stopped or cut, and the
one that finished it: a key that ran six hours over four jobs took six hours, not the forty
minutes of its last leg. `n` counts the keys that finished, and the medians and the 90th
percentile are over those keys' totals. `unfinished` counts keys with attempts on record and none
that finished: the classes too big for their request show up here rather than nowhere.

`cores` is the most common value among the attempts, `rss_peak` the largest peak of any attempt
(finished or not), `rss_process` whether any of those peaks is a whole process's rather than the
key's own, and `efficiency` the median fraction of its cores an attempt used (`NaN` when the CPU
time was not recorded).
"""
function cost_summary(costs::AbstractVector{KeyCost}; by=c -> c.class)
    groups = Dict{String,Vector{KeyCost}}()
    for c in costs
        push!(get!(Vector{KeyCost}, groups, String(by(c))), c)
    end
    out = Dict{String,NamedTuple}()
    for (label, cs) in groups
        # Attempts of one key, together.
        keys_ = Dict{Tuple{String,String},Vector{KeyCost}}()
        for c in cs
            push!(get!(Vector{KeyCost}, keys_, (c.stage, c.key)), c)
        end
        wall = Float64[]
        cpu = Float64[]
        unfinished = 0
        for attempts in values(keys_)
            if !any(a -> a.outcome == "ok", attempts)
                unfinished += 1
                continue
            end
            w = [a.wall for a in attempts if !isnan(a.wall)]
            isempty(w) && continue
            push!(wall, sum(w))
            c = [a.cpu for a in attempts]
            any(isnan, c) || push!(cpu, sum(c))
        end
        sort!(wall)
        sort!(cpu)
        eff = sort!([
            c.cpu / (c.wall * c.cores) for
            c in cs if !isnan(c.cpu) && c.cores > 0 && c.wall > 0
        ])
        counts = Dict{Int,Int}()
        for c in cs
            c.cores > 0 && (counts[c.cores] = get(counts, c.cores, 0) + 1)
        end
        out[label] = (;
            n=length(wall),
            wall_median=isempty(wall) ? NaN : _quantile(wall, 0.5),
            wall_p90=isempty(wall) ? NaN : _quantile(wall, 0.9),
            cpu_median=isempty(cpu) ? NaN : _quantile(cpu, 0.5),
            cores=isempty(counts) ? 0 : findmax(counts)[2],
            rss_peak=maximum(c.rss for c in cs),
            efficiency=isempty(eff) ? NaN : _quantile(eff, 0.5),
            unfinished=unfinished,
            rss_process=any(c -> c.rss_scope == "process", cs),
        )
    end
    return out
end

# ── the table the next job reads ────────────────────────────────────────────────────────────────

# JSON has no NaN: a number that is not there is `nothing`.
_json_value(x::AbstractFloat) = isfinite(x) ? x : nothing
_json_value(x) = x

"""
    cost_table_path(vault) -> String

`<state_root>/costs.json`.
"""
cost_table_path(vault::Vault) = joinpath(state_root(vault), "costs.json")

"""
    write_cost_table(vault) -> Dict{String,NamedTuple}

Summarise every key this stage has finished ([`cost_summary`](@ref) of [`key_costs`](@ref)) and
write it to [`cost_table_path`](@ref), atomically. [`run_loop!`](@ref) does this when it ends, so
the table is there for the next job; call it yourself after a bare `run!`.
"""
function write_cost_table(vault::Vault)
    summary = cost_summary(key_costs(vault))
    doc = Dict{String,Any}(
        "updated" => time(),
        "stage" => vault.run,
        "classes" => Dict{String,Any}(
            k => Dict{String,Any}(
                String(f) => _json_value(getfield(v, f)) for f in keys(v)
            ) for (k, v) in summary
        ),
    )
    atomic_write(io -> JSON3.write(io, doc), cost_table_path(vault))
    return summary
end

"""
    load_cost_table(vault; strict=false) -> Dict{String,NamedTuple}

The table [`write_cost_table`](@ref) left, by class; empty when there is none. A table that is
there and cannot be read is empty too, with a warning — or, with `strict=true`, an error, for a
caller that wants to say so itself. Only classes with at least one finished key are returned:
there is nothing to estimate from for the others.
"""
function load_cost_table(vault::Vault; strict::Bool=false)
    out = Dict{String,NamedTuple}()
    path = cost_table_path(vault)
    isfile(path) || return out
    try
        doc = JSON3.read(read(path, String), Dict{String,Any})
        for (k, v) in doc["classes"]
            num(f) = Float64(something(get(v, f, NaN), NaN))
            Int(v["n"]) > 0 || continue
            out[k] = (;
                n=Int(v["n"]),
                wall_median=num("wall_median"),
                wall_p90=num("wall_p90"),
                cpu_median=num("cpu_median"),
                cores=Int(v["cores"]),
                rss_peak=Int(v["rss_peak"]),
                efficiency=num("efficiency"),
                unfinished=Int(get(v, "unfinished", 0)),
                rss_process=get(v, "rss_process", false) === true,
            )
        end
    catch e
        e isa InterruptException && rethrow()
        strict && rethrow()
        @warn "SweepRunner: the cost table could not be read; estimates fall back to the application's" path exception =
            e maxlog = 3
        empty!(out)
    end
    return out
end

"""
    measured_cost(table, key_class; quantile=:median, fallback=key -> NaN) -> Function

`key -> seconds`: the wall time measured for the key's class (`:median` or `:p90`), and
`fallback(key)` — the application's own estimate — for a class the table has not seen. This is a
`cost` for [`run_campaign!`](@ref), [`remaining_work`](@ref) and [`campaign_work`](@ref) (wrap it
as `(stage, key) -> f(key)`), so what is left is counted from measurement.

`key_class` is the same function given to `run!`.
"""
function measured_cost(
    table::AbstractDict, key_class; quantile::Symbol=:median, fallback=key -> NaN
)
    quantile in (:median, :p90) ||
        throw(ArgumentError("measured_cost: quantile must be :median or :p90"))
    return key -> begin
        row = get(table, String(key_class(key)), nothing)
        row === nothing && return Float64(fallback(key))
        t = quantile === :median ? row.wall_median : row.wall_p90
        return isnan(t) ? Float64(fallback(key)) : t
    end
end

"""
    measured_mem(table, key_class; margin=1.2, fallback=key -> NaN) -> Function

`key -> bytes`: the largest peak RSS measured for the key's class times `margin`, and
`fallback(key)` for a class not seen yet. A memory request sized from this packs tighter than a
declared guess and is killed less often than one that was too small.
"""
function measured_mem(table::AbstractDict, key_class; margin::Real=1.2, fallback=key -> NaN)
    return key -> begin
        row = get(table, String(key_class(key)), nothing)
        (row === nothing || row.rss_peak == 0) && return Float64(fallback(key))
        return row.rss_peak * Float64(margin)
    end
end

"""
    measured_speedup(costs, key_class) -> Function

`(key, cores) -> speedup` relative to one core, from cost records taken at more than one thread
count: per class, the median wall time at the fewest cores measured over the median at the
largest measured count not above `cores`. A class measured at one count only, or not at all, is
`1.0`: no claim is made that threads help. This is the `speedup` of a [`SizedPool`](@ref) with
`threads = :finish_by`.
"""
function measured_speedup(costs::AbstractVector{KeyCost}, key_class)
    by = Dict{String,Dict{Int,Vector{Float64}}}()
    for c in costs
        (c.cores > 0 && !isnan(c.wall)) || continue
        push!(
            get!(Vector{Float64}, get!(Dict{Int,Vector{Float64}}, by, c.class), c.cores),
            c.wall,
        )
    end
    med = Dict(cls => Dict(n => _quantile(sort(w), 0.5) for (n, w) in d) for (cls, d) in by)
    return (key, cores) -> begin
        d = get(med, String(key_class(key)), nothing)
        (d === nothing || length(d) < 2) && return 1.0
        base = minimum(keys(d))
        at = [n for n in keys(d) if n <= cores]
        isempty(at) && return 1.0
        return d[base] / d[maximum(at)]
    end
end

"""
    print_costs([io], vault_or_outdir)

The per-class table: keys, median and p90 wall time, cores, how much of them was used, peak
memory.
"""
print_costs(x; kwargs...) = print_costs(stdout, x; kwargs...)

function print_costs(io::IO, x)
    summary = cost_summary(key_costs(x))
    if isempty(summary)
        println(io, "no finished keys on record")
        return nothing
    end
    println(
        io,
        rpad("class", 28),
        "    keys  median s     p90 s  cores  used  peak GB  unfinished",
    )
    for label in sort!(collect(keys(summary)))
        r = summary[label]
        used = isnan(r.efficiency) ? "-" : string(round(Int, 100 * r.efficiency), "%")
        println(
            io,
            rpad(isempty(label) ? "(all)" : label, 28),
            lpad(r.n, 8),
            lpad(round(r.wall_median; digits=1), 10),
            lpad(round(r.wall_p90; digits=1), 10),
            lpad(r.cores, 7),
            lpad(used, 6),
            lpad(round(r.rss_peak / 2^30; digits=2), 9),
            lpad(r.unfinished, 12),
        )
    end
    return nothing
end

export note_key!, KeyCost, key_costs, cost_summary, write_cost_table, load_cost_table
export measured_cost, measured_mem, measured_speedup, print_costs
