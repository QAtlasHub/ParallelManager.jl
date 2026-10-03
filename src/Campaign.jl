# Campaign — one file that says what a campaign runs.
#
# What a campaign ran was spread over places that did not know about each other: one config per
# (study, phase), a hard-coded list in the application's entry script of which studies "all"
# means, environment variables at submission for filters and priority, and per-partition
# conventions kept in shell history. Adding a study meant a new config AND an edit to the entry
# script AND a deploy, and nothing recorded which configs and filters a given job actually ran.
#
# A meta config names the configs:
#
#     [campaign]
#     name   = "2026-09"
#     outdir = "out/campaign"
#
#     [[study]]
#     name   = "conv"
#     stages = { phase1 = "conv_phase1.toml", phase2 = "conv_phase2.toml" }
#
#     [[study]]
#     name     = "typx"
#     stages   = { phase2 = "typx_phase2.toml" }
#     needs    = ["conv.phase1", "typ.phase1"]
#     priority = 10
#     enabled  = true
#
#     [profile.short]
#     max_key_time = "20min"
#     skip_stages  = ["phase1"]
#
# The application keeps the per-stage configs and the function that opens a stage (its `work_fn`,
# its vault); this file owns which stages run, in what order, under which filters.

using TOML
using SHA: sha256
using DataVault
using ParamIO
using ParamIO: DataKey

"""
    StageSpec

One stage of one study in a [`Campaign`](@ref): `study`, `name` (e.g. `"phase1"`), the absolute
path of its `config`, the stage ids it `needs` (`"study.stage"`), the study's `priority`, and
`extra` — whatever else the study's table held, passed through for the application.
"""
struct StageSpec
    study::String
    name::String
    config::String
    needs::Vector{String}
    priority::Int
    extra::Dict{String,Any}
end

"""
    stage_id(stage) -> String

`"<study>.<stage>"`, the name a stage is referred to by in `needs`, in `skip_stages` and in the
event log.
"""
stage_id(s::StageSpec) = string(s.study, ".", s.name)

"""
    CampaignProfile

A named set of filters for one kind of job (`[profile.<name>]`): `max_key_time` (seconds; keys
whose estimated cost is above it are left out, see `cost` in [`run_campaign!`](@ref)),
`skip_stages` (stage names or stage ids), `studies` (limit to these), `min_nodes`, and `extra`
for anything the application defines.
"""
struct CampaignProfile
    name::String
    max_key_time::Union{Float64,Nothing}
    skip_stages::Vector{String}
    studies::Union{Vector{String},Nothing}
    min_nodes::Union{Int,Nothing}
    extra::Dict{String,Any}
end

"""
    Campaign

A meta config, loaded: `path`, `name`, `outdir`, every [`StageSpec`](@ref) in file order
(`stages`), the study names (`studies`), which are `enabled`, the `profiles`, and the `sha256` of
the file as read. `problems` holds what loading found wrong; [`validate_campaign`](@ref) reports
them.
"""
struct Campaign
    path::String
    name::String
    outdir::String
    stages::Vector{StageSpec}
    studies::Vector{String}
    enabled::Dict{String,Bool}
    profiles::Dict{String,CampaignProfile}
    sha256::String
    problems::Vector{Finding}
end

# "phase2" before "phase10".
_natural(s::AbstractString) = replace(s, r"\d+" => m -> lpad(m, 12, '0'))

"""
    parse_duration(x) -> Float64

Seconds, from a number (already seconds) or a string like `"90s"`, `"20min"`, `"2h"`, `"1.5d"`.
A duration is finite and not negative; anything else is an `ArgumentError`.
"""
function parse_duration(x)
    if x isa Real
        (isfinite(x) && x >= 0) ||
            throw(ArgumentError("not a duration: $(repr(x)) (finite and >= 0)"))
        return Float64(x)
    end
    m = match(r"^\s*([0-9]*\.?[0-9]+)\s*([a-zA-Z]*)\s*$", String(x))
    m === nothing && throw(ArgumentError("not a duration: $(repr(x))"))
    v = parse(Float64, m[1])
    u = lowercase(m[2])
    u in ("", "s", "sec", "secs", "second", "seconds") && return v
    u in ("m", "min", "mins", "minute", "minutes") && return 60v
    u in ("h", "hr", "hrs", "hour", "hours") && return 3600v
    u in ("d", "day", "days") && return 86400v
    return throw(ArgumentError("not a duration: $(repr(x)) (unit $(repr(m[2])))"))
end

const _STUDY_KEYS = ("name", "stages", "priority", "needs", "enabled", "chain")
const _PROFILE_KEYS = ("max_key_time", "skip_stages", "studies", "min_nodes")

_bad(at, msg) = Finding(:campaign, :error, String(at), String(msg))
_warn(at, msg) = Finding(:campaign, :warn, String(at), String(msg))

# `stages`, as (name, config, needs) in the order they run within the study. A table is ordered by
# stage name (TOML tables carry no order); an array of tables is taken as written.
function _stage_entries(st, at, problems)
    out = Tuple{String,String,Vector{String}}[]
    if st isa AbstractDict
        for name in sort!(collect(keys(st)); by=_natural)
            v = st[name]
            if v isa AbstractString
                push!(out, (String(name), String(v), String[]))
            elseif v isa AbstractDict && haskey(v, "config")
                push!(
                    out, (String(name), String(v["config"]), String.(get(v, "needs", [])))
                )
            else
                push!(problems, _bad(at, "stage $name: expected a config path"))
            end
        end
    elseif st isa AbstractVector
        for v in st
            if v isa AbstractDict && haskey(v, "name") && haskey(v, "config")
                push!(
                    out,
                    (String(v["name"]), String(v["config"]), String.(get(v, "needs", []))),
                )
            else
                push!(problems, _bad(at, "a stage needs `name` and `config`"))
            end
        end
    else
        push!(problems, _bad(at, "`stages` must be a table or an array of tables"))
    end
    return out
end

"""
    load_campaign(path) -> Campaign

Read a meta config. Config paths are taken relative to the meta file (or to `[campaign]
config_dir`); `outdir` as written, relative to the working directory like a ParamIO `outdir`.

Within a study each stage needs the one before it (`chain = false` on the study turns that off).
`needs` on a study names what its FIRST stage needs from other studies, as `"study.stage"` or
`"study"` (all of that study's stages); a stage written as a table can carry its own `needs`.

Only a file that cannot be read or parsed throws. Everything else that is wrong with it — a
missing name, a `needs` that resolves to nothing — is kept in `problems` and reported by
[`validate_campaign`](@ref), so one pass shows all of it.
"""
function load_campaign(path::AbstractString)
    path = abspath(path)
    bytes = read(path)
    raw = TOML.parse(String(copy(bytes)))
    problems = Finding[]
    camp = get(raw, "campaign", Dict{String,Any}())
    haskey(raw, "campaign") || push!(problems, _bad("campaign", "no [campaign] table"))
    name = String(get(camp, "name", splitext(basename(path))[1]))
    outdir = abspath(String(get(camp, "outdir", "out")))
    cfgdir = normpath(joinpath(dirname(path), String(get(camp, "config_dir", "."))))

    stages = StageSpec[]
    studies = String[]
    enabled = Dict{String,Bool}()
    study_needs = Dict{String,Vector{String}}()
    for (i, st) in enumerate(get(raw, "study", Any[]))
        sname = get(st, "name", nothing)
        if !(sname isa AbstractString)
            push!(problems, _bad("study[$i]", "a study needs a `name`"))
            continue
        end
        sname = String(sname)
        if sname in studies
            push!(problems, _bad(sname, "study named twice"))
            continue
        end
        push!(studies, sname)
        enabled[sname] = get(st, "enabled", true) === true
        extra = Dict{String,Any}(k => v for (k, v) in st if !(k in _STUDY_KEYS))
        chain = get(st, "chain", true) === true
        prio = Int(get(st, "priority", 0))
        entries = _stage_entries(get(st, "stages", Dict{String,Any}()), sname, problems)
        isempty(entries) && push!(problems, _warn(sname, "study has no stages"))
        study_needs[sname] = String.(get(st, "needs", String[]))
        prev = nothing
        for (j, (stname, cfg, needs)) in enumerate(entries)
            n = copy(needs)
            j == 1 && append!(n, study_needs[sname])
            (chain && prev !== nothing) && push!(n, string(sname, ".", prev))
            cfgpath = isabspath(cfg) ? cfg : normpath(joinpath(cfgdir, cfg))
            push!(stages, StageSpec(sname, stname, cfgpath, n, prio, extra))
            prev = stname
        end
    end
    isempty(studies) && push!(problems, _bad("campaign", "no [[study]]"))

    # `needs` written as a study name means every stage of that study.
    ids = Set(stage_id(s) for s in stages)
    for s in stages
        resolved = String[]
        for n in s.needs
            if n in ids
                push!(resolved, n)
            elseif n in studies
                append!(resolved, [stage_id(t) for t in stages if t.study == n])
            else
                push!(
                    problems,
                    _bad(stage_id(s), "needs $(repr(n)), which is no stage or study"),
                )
            end
        end
        empty!(s.needs)
        append!(s.needs, unique(resolved))
    end

    profiles = Dict{String,CampaignProfile}()
    for (pname, p) in get(raw, "profile", Dict{String,Any}())
        mkt = try
            haskey(p, "max_key_time") ? parse_duration(p["max_key_time"]) : nothing
        catch e
            e isa ArgumentError || rethrow()
            push!(problems, _bad("profile.$pname", e.msg))
            nothing
        end
        mn = get(p, "min_nodes", nothing)
        if !(mn === nothing || (mn isa Integer && mn >= 1))
            push!(
                problems,
                _bad(
                    "profile.$pname", "min_nodes must be an integer >= 1, got $(repr(mn))"
                ),
            )
            mn = nothing
        end
        profiles[pname] = CampaignProfile(
            pname,
            mkt,
            String.(get(p, "skip_stages", String[])),
            haskey(p, "studies") ? String.(p["studies"]) : nothing,
            mn === nothing ? nothing : Int(mn),
            Dict{String,Any}(k => v for (k, v) in p if !(k in _PROFILE_KEYS)),
        )
    end
    return Campaign(
        path,
        name,
        outdir,
        stages,
        studies,
        enabled,
        profiles,
        bytes2hex(sha256(bytes)),
        problems,
    )
end

function _profile(c::Campaign, profile)
    profile === nothing && return nothing
    p = get(c.profiles, String(profile), nothing)
    p === nothing && throw(
        ArgumentError(
            "campaign $(c.name) has no profile $(repr(String(profile))); it has " *
            join(sort!(collect(keys(c.profiles))), ", "),
        ),
    )
    return p
end

function _skipped(p::CampaignProfile, s::StageSpec)
    return s.name in p.skip_stages || stage_id(s) in p.skip_stages
end

# A stage order in which every stage comes after what it needs, among `cands`; at each step the
# highest priority goes first, then file order. A stage inherits the priority of what needs it:
# "run this study first" would mean little if the stages it waits on stayed at the back.
# `nothing` when the needs form a cycle.
function _order(cands::Vector{StageSpec})
    ids = Set(stage_id(s) for s in cands)
    eff = Dict(stage_id(s) => s.priority for s in cands)
    for _ in 1:length(cands)                      # a chain is at most this long
        changed = false
        for s in cands, n in s.needs
            (n in ids && eff[n] < eff[stage_id(s)]) || continue
            eff[n] = eff[stage_id(s)]
            changed = true
        end
        changed || break
    end
    placed = Set{String}()
    out = StageSpec[]
    left = collect(enumerate(cands))
    while !isempty(left)
        ready = [(i, s) for (i, s) in left if all(n -> !(n in ids) || n in placed, s.needs)]
        isempty(ready) && return nothing
        sort!(ready; by=x -> (-eff[stage_id(x[2])], x[1]))
        i, s = first(ready)
        push!(out, s)
        push!(placed, stage_id(s))
        filter!(x -> x[1] != i, left)
    end
    return out
end

"""
    plan_campaign(campaign; studies=nothing, profile=nothing) -> Vector{StageSpec}

The stages a job runs, in order: the stages of the enabled studies (limited to `studies`, or to
the profile's `studies`), minus the profile's `skip_stages`, every stage after the stages it
needs, and among those that are ready the highest `priority` first, then file order.

A needed stage that is not in the plan (its study is disabled, or the profile skips it) is not
run by this job; [`run_campaign!`](@ref) checks that it is complete before running what needs it.
"""
function plan_campaign(c::Campaign; studies=nothing, profile=nothing)
    p = _profile(c, profile)
    want = if studies !== nothing
        String.(collect(studies))
    elseif p !== nothing && p.studies !== nothing
        p.studies
    else
        c.studies
    end
    unknown = setdiff(want, c.studies)
    isempty(unknown) ||
        throw(ArgumentError("campaign $(c.name) has no study $(join(unknown, ", "))"))
    cands = [
        s for s in c.stages if s.study in want &&
            get(c.enabled, s.study, false) &&
            !(p !== nothing && _skipped(p, s))
    ]
    order = _order(cands)
    order === nothing &&
        throw(ArgumentError("campaign $(c.name): the stages' `needs` form a cycle"))
    return order
end

"""
    validate_campaign(campaign) -> PreflightReport

Check the meta config before anything runs: what loading found (`problems`), every named config
exists and loads, `needs` resolve and do not form a cycle, a profile names only stages and studies
that exist, and no two stages with different configs write the same record — the same
`project_name` under the same stage name.

Gate on [`launchable`](@ref), as with any preflight.
"""
function validate_campaign(c::Campaign)
    fs = copy(c.problems)
    writers = Dict{Tuple{String,String},StageSpec}()
    for s in c.stages
        id = stage_id(s)
        if !isfile(s.config)
            push!(fs, _bad(id, "config not found: $(s.config)"))
            continue
        end
        spec = try
            ParamIO.load(s.config)
        catch e
            e isa InterruptException && rethrow()
            push!(fs, _bad(id, "config does not load: $(_short_err(e))"))
            continue
        end
        rec = (String(spec.study.project_name), s.name)
        other = get(writers, rec, nothing)
        if other === nothing
            writers[rec] = s
        elseif other.config != s.config
            push!(
                fs,
                _bad(
                    id,
                    "writes project $(rec[1]) run $(rec[2]), as $(stage_id(other)) does " *
                    "from a different config",
                ),
            )
        end
    end
    _order(c.stages) === nothing && push!(fs, _bad("campaign", "`needs` form a cycle"))
    names = Set(s.name for s in c.stages)
    ids = Set(stage_id(s) for s in c.stages)
    for (pname, p) in c.profiles
        for st in p.skip_stages
            (st in names || st in ids) || push!(
                fs, _warn("profile.$pname", "skip_stages names $(repr(st)), no stage")
            )
        end
        for st in something(p.studies, String[])
            st in c.studies ||
                push!(fs, _bad("profile.$pname", "studies names $(repr(st)), no study"))
        end
    end
    return PreflightReport(fs)
end

# ── opening a stage ─────────────────────────────────────────────────────────────────────────────

# What the application's `open_stage(stage)` returned, with the defaults filled in: the vault is
# the stage's config opened as run `<stage name>` under the campaign's outdir, the keys are that
# config's grid.
function _open(open_stage, c::Campaign, s::StageSpec; need_work::Bool=true)
    o = open_stage(s)
    o isa NamedTuple || throw(
        ArgumentError(
            "open_stage($(stage_id(s))) must return a NamedTuple with at least `work_fn`, " *
            "got $(typeof(o))",
        ),
    )
    (haskey(o, :work_fn) || !need_work) ||
        throw(ArgumentError("open_stage($(stage_id(s))) returned no `work_fn`"))
    vault = get(o, :vault, nothing)
    vault === nothing && (vault = DataVault.Vault(s.config; run=s.name, outdir=c.outdir))
    keys = get(o, :keys, nothing)
    keys === nothing && (keys = ParamIO.expand(vault.spec))
    return (;
        work_fn=get(o, :work_fn, nothing),
        vault=vault,
        keys=collect(DataKey, keys),
        load=get(o, :load, nothing),
        affinity=get(o, :affinity, nothing),
        prerequisite=get(o, :prerequisite, nothing),
        key_class=get(o, :key_class, nothing),
        min_time=get(o, :min_time, nothing),
        pool=get(o, :pool, nothing),
    )
end

# The keys of an opened stage that are not done: the manifest first, then the markers for what the
# manifest does not have (other masters' completions reach it only at the end of their stage).
function _undone(o)
    todo = todo_keys(load_manifest(o.vault), o.keys)
    return DataKey[k for k in todo if !DataVault.is_done(o.vault, k)]
end

"""
    remaining_work(open_stage, campaign; studies=nothing, profile=nothing, cost=nothing)
        -> Vector{NamedTuple}

What is left, per planned stage:
`(; stage, total, todo, eligible, cost, longest, unknown, blocked_by)`.
`todo` keys are not done; `eligible` of them pass the profile's `max_key_time`; `cost` is the sum
and `longest` the maximum of `cost(stage, key)` (seconds) over the eligible ones, `NaN` without a
`cost`; `blocked_by` lists the needed stages that are not complete (the stage cannot run until
they are).

This is the question a job — or whatever decides whether to submit one — asks of a campaign.
`open_stage` may leave `work_fn` out here, and `remaining_work(campaign; …)` opens every stage
with the defaults (its config as run `<stage name>` under the campaign's outdir).
"""
function remaining_work(
    open_stage, c::Campaign; studies=nothing, profile=nothing, cost=nothing
)
    # What is left of a campaign that could not be launched is not a number to size jobs by.
    report = validate_campaign(c)
    launchable(report) || throw(
        ArgumentError("campaign $(c.name) is not launchable:\n" * sprint(show, report))
    )
    p = _profile(c, profile)
    by_id = Dict(stage_id(s) => s for s in c.stages)
    undone = Dict{String,Vector{DataKey}}()
    opened = Dict{String,Any}()
    look(id) = get!(undone, id) do
        # Counting what is left needs the vault and the keys, not the work.
        o = get!(() -> _open(open_stage, c, by_id[id]; need_work=false), opened, id)
        return _undone(o)
    end
    out = NamedTuple[]
    for s in plan_campaign(c; studies, profile)
        id = stage_id(s)
        todo = look(id)
        elig = _eligible(todo, s, p, cost)
        known = if cost === nothing
            Float64[]
        else
            Float64[t for t in (key_seconds(k -> cost(s, k), k) for k in elig) if t !== nothing]
        end
        costs = known
        push!(
            out,
            (;
                stage=id,
                total=length(opened[id].keys),
                todo=length(todo),
                eligible=length(elig),
                cost=cost === nothing ? NaN : sum(costs; init=0.0),
                longest=cost === nothing ? NaN : maximum(costs; init=0.0),
                # Keys the cost hook has no answer for: in `eligible` only when no limit applies,
                # and not in `cost`.
                unknown=cost === nothing ? 0 : length(elig) - length(costs),
                blocked_by=String[n for n in s.needs if !isempty(look(n))],
            ),
        )
    end
    return out
end

remaining_work(c::Campaign; kwargs...) = remaining_work(s -> (;), c; kwargs...)

function _eligible(keys, s::StageSpec, p::Union{CampaignProfile,Nothing}, cost)
    (p === nothing || p.max_key_time === nothing) && return keys
    cost === nothing && throw(
        ArgumentError(
            "profile $(p.name) sets max_key_time, which needs `cost = (stage, key) -> seconds`",
        ),
    )
    # Asked the guarded way, as the run does. A key whose cost is not known is not assumed to be
    # short enough.
    return DataKey[
        k for k in keys if something(key_seconds(x -> cost(s, x), k), Inf) <= p.max_key_time
    ]
end

# ── running ─────────────────────────────────────────────────────────────────────────────────────

"""
    run_campaign!(open_stage, campaign; studies=nothing, profile=nothing, opts=RunOpts(),
                  cost=nothing, reload=true, loop=(;)) -> NamedTuple

Run the stages [`plan_campaign`](@ref) gives, each with [`run_loop!`](@ref).

`open_stage(stage::StageSpec)` is the application's half: it returns a NamedTuple with `work_fn`
and, optionally, `vault`, `keys`, `load`, `affinity`, `prerequisite`, `key_class`, `min_time`, `pool`. Left out, the vault is
`DataVault.Vault(stage.config; run=stage.name, outdir=campaign.outdir)` and the keys are that
config's whole grid.

Per stage:

- every stage it `needs` must be complete (all of its keys done). If one is not, the stage is not
  started and the result says which; running it would spend the allocation on keys whose inputs
  are known to be missing;
- with a profile that sets `max_key_time`, only the keys with `cost(stage, key) <= max_key_time`
  are run (`cost` returns seconds);
- `loop` is passed to `run_loop!` as keyword arguments (`max_empty_rounds`, `idle_sleep`, …).

With `reload=true` the meta file is re-read between stages when it has changed, so an edit to
`priority` or `enabled` reaches a campaign that is already running: the stages not yet started
are re-planned.

A stage that ends on a stop (`stop_flag`, `deadline`, a [`control!`](@ref) `:stop`) ends the
campaign: those bound the job, not the stage.

What the job ran is on record: `events_campaign_<host>_<pid>.jsonl` under the campaign's outdir
gets `campaign_start` (the meta file, its sha256, the profile, the stages in order), one
`campaign_stage` per stage (ran or why not, and its counts) and `campaign_done`.

Returns `(; campaign, profile, stages, stopped_by)`; `stages` is a vector of
`(; stage, ran, reason, keys, result)`.
"""
function run_campaign!(
    open_stage,
    c::Campaign;
    studies=nothing,
    profile=nothing,
    opts::RunOpts=RunOpts(),
    cost=nothing,
    reload::Bool=true,
    loop=(;),
)
    report = validate_campaign(c)
    launchable(report) || throw(
        ArgumentError("campaign $(c.name) is not launchable:\n" * sprint(show, report))
    )
    # A job the controller submitted is told its profile through the environment
    # (`SWEEPRUNNER_PROFILE`); an explicit `profile` wins.
    profile_source = profile === nothing ? "none" : "argument"
    if profile === nothing
        named = get(ENV, "SWEEPRUNNER_PROFILE", "")
        if !isempty(named)
            profile = named
            profile_source = "SWEEPRUNNER_PROFILE"
        end
    end
    p = _profile(c, profile)
    (p !== nothing && p.max_key_time !== nothing && cost === nothing) && throw(
        ArgumentError(
            "profile $(p.name) sets max_key_time, which needs `cost = (stage, key) -> seconds`",
        ),
    )
    log = EventLog(
        joinpath(c.outdir, "events_campaign_$(gethostname())_$(getpid()).jsonl");
        min_level=opts.log_level,
    )
    log_event(
        log,
        :campaign_start;
        campaign=c.name,
        meta=c.path,
        sha256=c.sha256,
        profile=p === nothing ? nothing : p.name,
        profile_source=profile_source,
        studies=studies === nothing ? nothing : String.(collect(studies)),
        stages=stage_id.(plan_campaign(c; studies, profile)),
    )
    # `min_nodes` says what size of job a profile is for. A smaller allocation running it is
    # said; it is not refused, since the keys it selects are still valid work.
    nodes = tryparse(Int, get(ENV, "SLURM_JOB_NUM_NODES", ""))
    if p !== nothing && p.min_nodes !== nothing && nodes !== nothing && nodes < p.min_nodes
        log_event(
            log,
            :profile_too_small;
            level=:warn,
            campaign=c.name,
            profile=p.name,
            min_nodes=p.min_nodes,
            nodes=nodes,
        )
    end

    results = NamedTuple[]
    seen = Set{String}()
    complete = Set{String}()
    opened = Dict{String,Any}()
    stopped = nothing
    open_id(c, id) = get!(opened, id) do
        s = c.stages[findfirst(t -> stage_id(t) == id, c.stages)]
        return _open(open_stage, c, s)
    end
    is_complete_stage(c, id) = id in complete || begin
        done = isempty(_undone(open_id(c, id)))
        done && push!(complete, id)
        done
    end

    while true
        if reload
            c2 = _reload(c, log)
            c2 === nothing || (c=c2; empty!(opened))
            p = _profile(c, profile)
        end
        plan = plan_campaign(c; studies, profile)
        i = findfirst(s -> !(stage_id(s) in seen), plan)
        i === nothing && break
        s = plan[i]
        id = stage_id(s)
        push!(seen, id)

        blocked = String[n for n in s.needs if !is_complete_stage(c, n)]
        if !isempty(blocked)
            # With what went wrong there, when this campaign ran it: "needs X" alone reads as
            # "X has not run yet".
            reason = "needs " * join((_need_note(n, results) for n in blocked), ", ")
            log_event(log, :campaign_stage; stage=id, ran=false, reason=reason)
            push!(results, (; stage=id, ran=false, reason=reason, keys=0, result=nothing))
            continue
        end

        o = open_id(c, id)
        keys = _eligible(o.keys, s, p, cost)
        r = run_loop!(
            o.work_fn,
            o.vault,
            keys;
            opts=opts,
            load=o.load,
            affinity=o.affinity,
            prerequisite=o.prerequisite,
            key_class=o.key_class,
            cost=cost === nothing ? nothing : (k -> cost(s, k)),
            min_time=o.min_time,
            pool=o.pool,
            loop...,
        )
        log_event(
            log,
            :campaign_stage;
            stage=id,
            ran=r.ran,
            reason=r.ran ? nothing : "prerequisite",
            keys=length(keys),
            of=length(o.keys),
            done=r.done,
            busy=r.busy,
            err=r.err,
            gave_up=r.gave_up,
            remaining=r.remaining,
            rounds=r.rounds,
            stopped_by=r.stopped_by === nothing ? nothing : String(r.stopped_by),
        )
        push!(
            results,
            (;
                stage=id,
                ran=r.ran,
                reason=r.ran ? nothing : "prerequisite",
                keys=length(keys),
                result=r,
            ),
        )
        if r.stopped_by !== nothing
            stopped = r.stopped_by
            break
        end
    end
    log_event(
        log,
        :campaign_done;
        campaign=c.name,
        stages=length(results),
        ran=count(x -> x.ran, results),
        stopped_by=stopped === nothing ? nothing : String(stopped),
    )
    return (;
        campaign=c.name,
        profile=p === nothing ? nothing : p.name,
        stages=results,
        stopped_by=stopped,
    )
end

# A needed stage, with how it ended if this campaign ran it and it left keys failing.
function _need_note(id::AbstractString, results)
    i = findfirst(x -> x.stage == id && x.result !== nothing, results)
    i === nothing && return String(id)
    r = results[i].result
    r.err > 0 || return String(id)
    return "$id ($(r.err) of its keys failed, $(r.remaining) not done)"
end

# `c`, remembering `sha` as the version of the file it has looked at.
function _with_sha(c::Campaign, sha::AbstractString)
    return Campaign(
        c.path,
        c.name,
        c.outdir,
        c.stages,
        c.studies,
        c.enabled,
        c.profiles,
        String(sha),
        c.problems,
    )
end

# The meta file re-read, when its bytes changed: the new campaign if it is launchable, the old one
# (remembering the hash it refused, so the refusal is said once) if it is not, `nothing` when the
# file is unchanged. A broken edit under a running campaign is reported and ignored, not fatal.
function _reload(c::Campaign, log::EventLog)
    sha = try
        bytes2hex(sha256(read(c.path)))
    catch e
        e isa InterruptException && rethrow()
        return nothing                              # unreadable right now: look again later
    end
    sha == c.sha256 && return nothing
    c2 = try
        load_campaign(c.path)
    catch e
        e isa InterruptException && rethrow()
        log_event(
            log,
            :campaign_reload_refused;
            level=:warn,
            meta=c.path,
            sha256=sha,
            err=_short_err(e),
        )
        return _with_sha(c, sha)
    end
    report = validate_campaign(c2)
    if !launchable(report)
        log_event(
            log,
            :campaign_reload_refused;
            level=:warn,
            meta=c.path,
            sha256=sha,
            err="$(n_errors(report)) error(s) in the edited file",
        )
        return _with_sha(c, sha)
    end
    log_event(log, :campaign_reloaded; meta=c.path, sha256=c2.sha256)
    return c2
end

function Base.show(io::IO, c::Campaign)
    println(io, "Campaign ", c.name, "  (", c.path, ")")
    println(io, "  outdir ", c.outdir)
    for st in c.studies
        on = get(c.enabled, st, false) ? "" : "  [disabled]"
        ss = [s for s in c.stages if s.study == st]
        prio = isempty(ss) || ss[1].priority == 0 ? "" : "  priority $(ss[1].priority)"
        println(io, "  study ", st, prio, on)
        for s in ss
            needs = isempty(s.needs) ? "" : "  needs " * join(s.needs, ", ")
            println(io, "    ", s.name, "  ", basename(s.config), needs)
        end
    end
    for name in sort!(collect(keys(c.profiles)))
        println(io, "  profile ", name)
    end
    return nothing
end

export Campaign, StageSpec, CampaignProfile, stage_id, load_campaign, plan_campaign
export validate_campaign, run_campaign!, remaining_work
