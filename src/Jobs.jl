# Jobs — deciding submissions with what the sweep knows.
#
# A master runs inside an allocation someone else made. How many jobs to submit, on which
# partitions, whether to resubmit when one ends, and whether the budget allows it were decided
# outside it, by shell loops and by hand, without the one thing that should drive them: what is
# left. A loop that kept a short queue busy went on submitting after the eligible work had run
# out; a chain script resubmitted a fixed number of times whatever remained.
#
# Three pieces, each usable alone:
#
#   Scheduler   `submit / cancel / job_states / remaining_time / shrink`, with SLURM as the first
#               backend and a mock, so a policy is testable without a cluster.
#   decide      a pure function: (policy, what is left, the jobs that exist, the ledger) -> what
#               to submit and what not to, each with its reason.
#   Ledger      node-hours used and committed, on disk, checked against a hard budget.
#
# A system that submits charged jobs on its own has to be explicit: `dry_run` is the default,
# every decision is logged with its reason, and the budget is a refusal, not a warning.

using JSON3
using TOML

# ── the scheduler behind an interface ───────────────────────────────────────────────────────────

"""
    Scheduler

What job management needs from a batch system. A backend implements

- `submit(s, spec::JobSpec) -> String` — the job id;
- `cancel(s, id) -> Bool`;
- `job_states(s) -> Vector{JobState}` — this user's jobs that are pending or running;
- `remaining_time(s, id) -> Union{Float64,Nothing}` — seconds left, `nothing` when unknown;
- `shrink(s, id, nodes) -> Bool` — give nodes back, `false` where the scheduler cannot.

[`SlurmScheduler`](@ref) and [`MockScheduler`](@ref) are the two provided.
"""
abstract type Scheduler end

"""
    JobSpec(; name, partition, nodes, time_limit, script, env=Dict(), args=String[])

One submission: `time_limit` in seconds, `script` the batch script, `env` exported to it.
"""
Base.@kwdef struct JobSpec
    name::String
    partition::String
    nodes::Int
    time_limit::Float64
    script::String
    env::Dict{String,String} = Dict{String,String}()
    args::Vector{String} = String[]
end

"""
    JobState

A job as the scheduler reports it: `id`, `name`, `partition`, `state` (`:pending`, `:running`,
or `:other`), `nodes`, `time_limit` and `elapsed` in seconds.
"""
struct JobState
    id::String
    name::String
    partition::String
    state::Symbol
    nodes::Int
    time_limit::Float64
    elapsed::Float64
end

"""
    submit(scheduler, spec::JobSpec) -> String

Submit one job and return the scheduler's id for it. Throws when the scheduler refuses.
"""
function submit end

"""
    cancel(scheduler, id) -> Bool
"""
function cancel end

"""
    job_states(scheduler) -> Vector{JobState}

This user's jobs that the scheduler still lists. Throws when the scheduler cannot be asked: "no
jobs" and "no answer" must not look the same to something that submits.
"""
function job_states end

"""
    remaining_time(scheduler, id) -> Union{Float64,Nothing}

Seconds until job `id` reaches its time limit, or `nothing` when that is not known.
"""
remaining_time(::Scheduler, ::AbstractString) = nothing

"""
    shrink(scheduler, id, nodes) -> Bool

Reduce job `id` to `nodes` nodes, giving the rest back; `false` where the scheduler cannot. Pair
it with a `:drain` request for the nodes that are to go, so nothing is dispatched to them first.
"""
shrink(::Scheduler, ::AbstractString, ::Integer) = false

# `D-HH:MM:SS`, `HH:MM:SS`, `MM:SS`, `SS` as Slurm prints them; `Inf` for UNLIMITED, `0` for what
# it cannot read (a job that has not started prints `0:00` or `N/A`).
function _slurm_seconds(s::AbstractString)::Float64
    s = strip(s)
    (isempty(s) || s in ("N/A", "INVALID", "NOT_SET")) && return 0.0
    s == "UNLIMITED" && return Inf
    days = 0.0
    if occursin('-', s)
        d, s = split(s, '-'; limit=2)
        days = something(tryparse(Float64, d), 0.0)
    end
    parts = [something(tryparse(Float64, p), 0.0) for p in split(s, ':')]
    secs = 0.0
    for p in parts
        secs = 60secs + p
    end
    return 86400days + secs
end

# Seconds as `sbatch -t` takes them: whole minutes, rounded up.
_slurm_minutes(secs::Real) = string(max(1, ceil(Int, secs / 60)))

# Run a command with a bound and return its stdout, or `nothing` if it failed or timed out.
function _run_command(cmd::Cmd; timeout::Real=60.0)::Union{String,Nothing}
    out = tempname()
    proc = try
        run(pipeline(cmd; stdout=out, stderr=devnull); wait=false)
    catch e
        e isa InterruptException && rethrow()
        return nothing
    end
    try
        if timedwait(() -> !process_running(proc), Float64(timeout); pollint=0.05) !== :ok
            kill(proc, Base.SIGKILL)
            return nothing
        end
        return success(proc) ? read(out, String) : nothing
    finally
        rm(out; force=true)
    end
end

"""
    SlurmScheduler(; user=ENV["USER"], run=<run a Cmd, return its stdout or nothing>)

[`Scheduler`](@ref) over `sbatch`, `scancel`, `squeue` and `scontrol`. `run` is the one place a
command is executed; a test passes its own to see the command lines and answer for the cluster.
"""
struct SlurmScheduler <: Scheduler
    user::String
    run::Any
end

function SlurmScheduler(; user::AbstractString=get(ENV, "USER", ""), run=_run_command)
    return SlurmScheduler(String(user), run)
end

function submit(s::SlurmScheduler, spec::JobSpec)::String
    exports = join(["ALL"; ["$k=$v" for (k, v) in sort!(collect(spec.env))]], ",")
    mins = _slurm_minutes(spec.time_limit)
    cmd = `sbatch --parsable -J $(spec.name) -p $(spec.partition) -N $(spec.nodes) -t $mins --export=$exports $(spec.script) $(spec.args)`
    out = s.run(cmd)
    out === nothing && error("sbatch failed for job $(spec.name) on $(spec.partition)")
    id = strip(first(split(strip(out), ';')))
    isempty(id) && error("sbatch printed no job id for $(spec.name)")
    return String(id)
end

cancel(s::SlurmScheduler, id::AbstractString) = s.run(`scancel $id`) !== nothing

function job_states(s::SlurmScheduler)::Vector{JobState}
    out = s.run(`squeue -h -u $(s.user) -o "%i|%j|%P|%T|%D|%l|%M"`)
    out === nothing && error("squeue failed: the jobs that exist are not known")
    jobs = JobState[]
    for line in split(out, '\n'; keepempty=false)
        f = split(strip(line, ['"', ' ']), '|')
        length(f) == 7 || continue
        state = if f[4] == "RUNNING"
            :running
        elseif f[4] == "PENDING"
            :pending
        else
            :other
        end
        push!(
            jobs,
            JobState(
                String(f[1]),
                String(f[2]),
                String(f[3]),
                state,
                something(tryparse(Int, f[5]), 0),
                _slurm_seconds(f[6]),
                _slurm_seconds(f[7]),
            ),
        )
    end
    return jobs
end

function remaining_time(s::SlurmScheduler, id::AbstractString)
    out = s.run(`squeue -h -j $id -o %L`)
    (out === nothing || isempty(strip(out))) && return nothing
    return _slurm_seconds(out)
end

function shrink(s::SlurmScheduler, id::AbstractString, nodes::Integer)
    return s.run(`scontrol update JobId=$id NumNodes=$nodes`) !== nothing
end

"""
    MockScheduler()

An in-memory [`Scheduler`](@ref): `submit` appends to `jobs` (as pending) and to `submitted`,
`cancel` removes. A test moves the jobs along by editing `jobs`.
"""
mutable struct MockScheduler <: Scheduler
    jobs::Vector{JobState}
    submitted::Vector{JobSpec}
    next::Int
end

MockScheduler() = MockScheduler(JobState[], JobSpec[], 1000)

function submit(s::MockScheduler, spec::JobSpec)::String
    id = string(s.next += 1)
    push!(s.submitted, spec)
    push!(
        s.jobs,
        JobState(id, spec.name, spec.partition, :pending, spec.nodes, spec.time_limit, 0.0),
    )
    return id
end

function cancel(s::MockScheduler, id::AbstractString)
    n = length(s.jobs)
    filter!(j -> j.id != id, s.jobs)
    return length(s.jobs) < n
end

job_states(s::MockScheduler) = copy(s.jobs)

function remaining_time(s::MockScheduler, id::AbstractString)
    i = findfirst(j -> j.id == id, s.jobs)
    return i === nothing ? nothing : s.jobs[i].time_limit - s.jobs[i].elapsed
end

# ── the policy ──────────────────────────────────────────────────────────────────────────────────

"""
    PartitionPolicy(; name, nodes, time_limit, script, profile=nothing, max_jobs=1,
                    slots_per_node=1, key_time=nothing, env=Dict())

What a job on one partition looks like and how many of them may exist: `nodes` and `time_limit`
(seconds) per job, the batch `script`, the campaign `profile` such a job runs (it decides which
work counts as runnable here), at most `max_jobs` pending or running, `slots_per_node` workers
per node (how much work a job can take), and `key_time`, the seconds one unit is assumed to take
when there is no cost model.
"""
Base.@kwdef struct PartitionPolicy
    name::String
    nodes::Int
    time_limit::Float64
    script::String
    profile::Union{String,Nothing} = nothing
    max_jobs::Int = 1
    slots_per_node::Int = 1
    key_time::Union{Float64,Nothing} = nothing
    env::Dict{String,String} = Dict{String,String}()
end

"""
    JobPolicy(; name, partitions, budget_node_hours, max_jobs=typemax(Int), dry_run=true,
              default_key_time=600.0)

The rules a controller submits under. `name` prefixes every job it submits and is how it
recognises its own. `budget_node_hours` is hard: a submission that would take used plus
committed node-hours past it is refused. `dry_run` (the default) decides and logs but submits
nothing.
"""
Base.@kwdef struct JobPolicy
    name::String
    partitions::Vector{PartitionPolicy}
    budget_node_hours::Float64
    max_jobs::Int = typemax(Int)
    dry_run::Bool = true
    default_key_time::Float64 = 600.0
end

"""
    load_job_policy(path) -> JobPolicy

Read a policy from the `[jobs]` table of a TOML file — typically the campaign's meta config, so
what runs and how it is submitted are one file:

```toml
[jobs]
name              = "ft"
budget_node_hours = 5000
max_jobs          = 8
dry_run           = true

[[jobs.partition]]
name           = "i8cpu"
nodes          = 8
time_limit     = "30min"
script         = "batch/run.sh"
profile        = "short"
max_jobs       = 1
slots_per_node = 32
```

Script paths are relative to the file.
"""
function load_job_policy(path::AbstractString)
    path = abspath(path)
    raw = TOML.parsefile(path)
    j = get(raw, "jobs", nothing)
    j === nothing && throw(ArgumentError("$path has no [jobs] table"))
    haskey(j, "budget_node_hours") ||
        throw(ArgumentError("$path: [jobs] needs budget_node_hours; there is no default"))
    parts = PartitionPolicy[]
    for p in get(j, "partition", Any[])
        script = String(p["script"])
        isabspath(script) || (script = normpath(joinpath(dirname(path), script)))
        push!(
            parts,
            PartitionPolicy(;
                name=String(p["name"]),
                nodes=Int(p["nodes"]),
                time_limit=parse_duration(p["time_limit"]),
                script=script,
                profile=haskey(p, "profile") ? String(p["profile"]) : nothing,
                max_jobs=Int(get(p, "max_jobs", 1)),
                slots_per_node=Int(get(p, "slots_per_node", 1)),
                key_time=haskey(p, "key_time") ? parse_duration(p["key_time"]) : nothing,
                env=Dict{String,String}(
                    String(k) => string(v) for (k, v) in get(p, "env", Dict{String,Any}())
                ),
            ),
        )
    end
    isempty(parts) && throw(ArgumentError("$path: [jobs] names no [[jobs.partition]]"))
    return JobPolicy(;
        name=String(get(j, "name", "sweep")),
        partitions=parts,
        budget_node_hours=Float64(j["budget_node_hours"]),
        max_jobs=Int(get(j, "max_jobs", typemax(Int))),
        dry_run=get(j, "dry_run", true) === true,
        default_key_time=parse_duration(get(j, "default_key_time", 600.0)),
    )
end

# ── the ledger ──────────────────────────────────────────────────────────────────────────────────

"""
    Ledger(path)

Every job a controller submitted — partition, nodes, time limit, when, how long it has run,
whether it has ended — kept in one JSON file so that the budget holds across controller restarts.
[`node_hours`](@ref) is the account.
"""
mutable struct Ledger
    path::String
    jobs::Dict{String,Dict{String,Any}}
end

function Ledger(path::AbstractString)
    jobs = Dict{String,Dict{String,Any}}()
    if isfile(path)
        raw = JSON3.read(read(path, String), Dict{String,Any})
        for (id, j) in get(raw, "jobs", Dict{String,Any}())
            jobs[String(id)] = Dict{String,Any}(j)
        end
    end
    return Ledger(String(path), jobs)
end

function save_ledger(l::Ledger)
    atomic_write(io -> JSON3.write(io, Dict("jobs" => l.jobs)), l.path)
    return l.path
end

function record_submit!(l::Ledger, id::AbstractString, spec::JobSpec; now::Real=time())
    l.jobs[String(id)] = Dict{String,Any}(
        "name" => spec.name,
        "partition" => spec.partition,
        "nodes" => spec.nodes,
        "time_limit" => spec.time_limit,
        "submitted" => Float64(now),
        "elapsed" => 0.0,
        "state" => "pending",
        "ended" => false,
    )
    return nothing
end

"""
    observe!(ledger, states)

Bring the ledger up to what the scheduler says: a job it still lists has its elapsed time and
state updated; one it no longer lists has ended, with the elapsed time last seen.
"""
function observe!(l::Ledger, states::AbstractVector{JobState})
    by = Dict(j.id => j for j in states)
    for (id, j) in l.jobs
        j["ended"] === true && continue
        s = get(by, id, nothing)
        if s === nothing || s.state === :other
            j["ended"] = true
            j["state"] = "ended"
        else
            j["elapsed"] = max(Float64(j["elapsed"]), s.elapsed)
            j["state"] = String(s.state)
        end
    end
    return nothing
end

"""
    node_hours(ledger) -> NamedTuple

`(; used, committed, by_partition)`: node-hours the ledger's jobs have run, node-hours its live
jobs can still run (to their time limits), and `partition => (; used, committed)`.
"""
function node_hours(l::Ledger)
    used = committed = 0.0
    by = Dict{String,Tuple{Float64,Float64}}()
    for j in values(l.jobs)
        u = Float64(j["nodes"]) * Float64(j["elapsed"]) / 3600
        c = if j["ended"] === true
            0.0
        else
            Float64(j["nodes"]) * max(Float64(j["time_limit"]) - Float64(j["elapsed"]), 0.0) / 3600
        end
        used += u
        committed += c
        pu, pc = get(by, j["partition"], (0.0, 0.0))
        by[j["partition"]] = (pu + u, pc + c)
    end
    return (;
        used=used,
        committed=committed,
        by_partition=Dict(k => (; used=v[1], committed=v[2]) for (k, v) in by),
    )
end

# ── deciding ────────────────────────────────────────────────────────────────────────────────────

"""
    Decision

One thing [`decide`](@ref) concluded for a partition: `action` is `:submit` (with `spec`),
`:hold` (nothing to do, and why) or `:refuse` (it would have submitted, and may not), each with
its `reason` in words and the `node_hours` a submission commits.
"""
struct Decision
    action::Symbol
    partition::String
    reason::String
    spec::Union{JobSpec,Nothing}
    node_hours::Float64
end

"""
    campaign_work(open_stage, campaign; cost=nothing) -> Function

The `work` function [`decide`](@ref) asks: `profile -> (; units, cost, longest)` over the stages
of `campaign` that can run now under that profile — undone keys the profile lets a job take, in
stages whose needs are complete. `cost` and `longest` are seconds, `NaN` without a cost model.
"""
function campaign_work(open_stage, c::Campaign; cost=nothing)
    return profile -> begin
        rows = remaining_work(open_stage, c; profile=profile, cost=cost)
        can = [r for r in rows if isempty(r.blocked_by)]
        return (;
            units=sum(r -> r.eligible, can; init=0),
            cost=cost === nothing ? NaN : sum(r -> r.cost, can; init=0.0),
            longest=cost === nothing ? NaN : maximum(r -> r.longest, can; init=0.0),
        )
    end
end

"""
    decide(policy, work, jobs, ledger) -> Vector{Decision}

What to submit, given `work(profile) -> (; units, cost, longest)` (what is runnable under a
partition's profile; see [`campaign_work`](@ref)), the `jobs` the scheduler lists, and the
ledger. Pure: nothing is submitted or written.

Per partition, in the policy's order:

- nothing runnable under its profile → `:hold`. This is the rule a keep-the-queue-busy loop
  lacks: it does not submit a job that would start, find nothing and exit.
- the jobs already pending or running there can take what is left (their remaining worker-time
  covers the estimated cost, or they have as many worker slots as there are units) → `:hold`.
- otherwise as many jobs as the remaining cost needs, never more worker slots than units, within
  the partition's and the policy's `max_jobs`;
- each submission is checked against the budget: used + committed + this job's node-hours must
  not exceed `budget_node_hours`, else `:refuse`.
"""
function decide(
    policy::JobPolicy, work, jobs::AbstractVector{JobState}, ledger::Ledger
)::Vector{Decision}
    out = Decision[]
    mine = [
        j for j in jobs if (j.state === :pending || j.state === :running) &&
            (haskey(ledger.jobs, j.id) || startswith(j.name, policy.name))
    ]
    nh = node_hours(ledger)
    spent = nh.used + nh.committed
    live_total = length(mine)
    for p in policy.partitions
        w = work(p.profile)
        live = [j for j in mine if j.partition == p.name]
        prof = p.profile === nothing ? "no profile" : "profile $(p.profile)"
        if w.units == 0
            push!(
                out, Decision(:hold, p.name, "nothing runnable under $prof", nothing, 0.0)
            )
            continue
        end
        key_time = something(p.key_time, policy.default_key_time)
        cost = isnan(w.cost) ? w.units * key_time : w.cost
        have = sum(
            j -> j.nodes * p.slots_per_node * max(j.time_limit - j.elapsed, 0.0),
            live;
            init=0.0,
        )
        live_slots = sum(j -> j.nodes * p.slots_per_node, live; init=0)
        if cost <= have || live_slots >= w.units
            push!(
                out,
                Decision(
                    :hold,
                    p.name,
                    "$(length(live)) job(s) there already cover $(w.units) unit(s)",
                    nothing,
                    0.0,
                ),
            )
            continue
        end
        slots = p.nodes * p.slots_per_node
        n = ceil(Int, (cost - have) / (slots * p.time_limit))
        n = min(n, ceil(Int, (w.units - live_slots) / slots))
        room = min(p.max_jobs - length(live), policy.max_jobs - live_total)
        if room <= 0
            push!(
                out,
                Decision(
                    :hold,
                    p.name,
                    "max_jobs reached ($(length(live)) here, $live_total in all)",
                    nothing,
                    0.0,
                ),
            )
            continue
        end
        for _ in 1:min(n, room)
            job_nh = p.nodes * p.time_limit / 3600
            if spent + job_nh > policy.budget_node_hours
                push!(
                    out,
                    Decision(
                        :refuse,
                        p.name,
                        "budget: $(round(spent; digits=1)) node-hours used or committed, " *
                        "this job needs $(round(job_nh; digits=1)), the budget is " *
                        "$(policy.budget_node_hours)",
                        nothing,
                        job_nh,
                    ),
                )
                break
            end
            spent += job_nh
            live_total += 1
            env = copy(p.env)
            p.profile === nothing || (env["SWEEPRUNNER_PROFILE"] = p.profile)
            spec = JobSpec(;
                name=string(policy.name, "-", p.name),
                partition=p.name,
                nodes=p.nodes,
                time_limit=p.time_limit,
                script=p.script,
                env=env,
            )
            reason =
                "$(w.units) unit(s) runnable under $prof, about " *
                "$(round(cost / 3600; digits=1)) worker-hours; $(length(live)) job(s) there"
            push!(out, Decision(:submit, p.name, reason, spec, job_nh))
        end
    end
    return out
end

# ── acting ──────────────────────────────────────────────────────────────────────────────────────

"""
    JobController(scheduler, policy, outdir)

A [`Scheduler`](@ref), a [`JobPolicy`](@ref) and where the record goes: the ledger at
`<outdir>/sweeprunner/jobs/ledger.json` and the event log
`<outdir>/events_jobs_<host>_<pid>.jsonl`. It can live on a login node, outliving any one job, or
be called by a job as its last act.
"""
struct JobController
    scheduler::Scheduler
    policy::JobPolicy
    ledger::Ledger
    log::EventLog
end

function JobController(scheduler::Scheduler, policy::JobPolicy, outdir::AbstractString)
    return JobController(
        scheduler,
        policy,
        Ledger(joinpath(outdir, "sweeprunner", "jobs", "ledger.json")),
        EventLog(joinpath(outdir, "events_jobs_$(gethostname())_$(getpid()).jsonl")),
    )
end

"""
    manage!(controller, work) -> Vector{Decision}

One round: ask the scheduler which jobs exist, bring the ledger up to date, [`decide`](@ref), and
carry the decisions out — unless the policy is `dry_run`, in which case they are only logged.
Every decision goes to the event log (`job_decision`, with its reason; `job_submitted` with the
id), and the ledger is saved.

Call it from a job that is ending to resubmit only if work remains, or in
[`controller_loop!`](@ref) to keep a campaign supplied.
"""
function manage!(ctl::JobController, work)::Vector{Decision}
    states = job_states(ctl.scheduler)
    observe!(ctl.ledger, states)
    decisions = decide(ctl.policy, work, states, ctl.ledger)
    for d in decisions
        log_event(
            ctl.log,
            :job_decision;
            action=String(d.action),
            partition=d.partition,
            reason=d.reason,
            node_hours=d.node_hours,
            dry_run=ctl.policy.dry_run,
        )
        (d.action === :submit && !ctl.policy.dry_run) || continue
        id = try
            submit(ctl.scheduler, d.spec)
        catch e
            e isa InterruptException && rethrow()
            log_event(
                ctl.log,
                :job_submit_failed;
                level=:warn,
                partition=d.partition,
                err=_short_err(e),
            )
            continue
        end
        record_submit!(ctl.ledger, id, d.spec)
        log_event(
            ctl.log,
            :job_submitted;
            id=id,
            partition=d.partition,
            nodes=d.spec.nodes,
            time_limit=d.spec.time_limit,
            node_hours=d.node_hours,
        )
    end
    save_ledger(ctl.ledger)
    return decisions
end

"""
    controller_loop!(controller, work; interval=300.0, stop=() -> false, max_rounds=nothing)

[`manage!`](@ref) every `interval` seconds until `stop()` is true, `max_rounds` have run, or
nothing is left: no runnable work on any partition and no job of ours pending or running. A
`dry_run` policy runs one round: nothing it decides changes what the next round would see. Returns
the number of rounds.
"""
function controller_loop!(
    ctl::JobController,
    work;
    interval::Real=300.0,
    stop=() -> false,
    max_rounds::Union{Integer,Nothing}=nothing,
)
    rounds = 0
    while !stop()
        rounds += 1
        decisions = manage!(ctl, work)
        live = any(j -> j["ended"] !== true, values(ctl.ledger.jobs))
        idle = all(
            d -> d.action === :hold && startswith(d.reason, "nothing runnable"), decisions
        )
        (idle && !live) && break
        # A dry run submits nothing, so a second round would decide the same thing forever.
        ctl.policy.dry_run && break
        (max_rounds !== nothing && rounds >= max_rounds) && break
        sleep(interval)
    end
    return rounds
end

"""
    print_decisions([io], decisions, ledger, policy)

The decisions and the account, for a human: what would be (or was) submitted, what was held or
refused and why, and node-hours used and committed against the budget.
"""
function print_decisions(io::IO, ds::AbstractVector{Decision}, l::Ledger, policy::JobPolicy)
    nh = node_hours(l)
    println(
        io,
        "budget   $(round(nh.used; digits=1)) used + $(round(nh.committed; digits=1)) ",
        "committed of $(policy.budget_node_hours) node-hours",
        policy.dry_run ? "   (dry run: nothing is submitted)" : "",
    )
    for d in ds
        println(io, "  ", rpad(d.action, 7), rpad(d.partition, 12), d.reason)
    end
    return nothing
end

print_decisions(ds, l, policy) = print_decisions(stdout, ds, l, policy)

# Exported: the types and the entry points whose names say what they are. `submit`, `cancel`,
# `job_states`, `remaining_time`, `shrink`, `observe!`, `record_submit!`, `save_ledger`, `Ledger`,
# `node_hours`, `Decision`, `decide`, `manage!` and `print_decisions` are used qualified
# (`SweepRunner.decide`): names that short are not this package's to claim in a caller's namespace.
export Scheduler, JobSpec, JobState, SlurmScheduler, MockScheduler
export PartitionPolicy, JobPolicy, load_job_policy
export campaign_work, JobController, controller_loop!
