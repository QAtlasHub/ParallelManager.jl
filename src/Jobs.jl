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
    # The numbers a submission is charged by are checked where they enter: a NaN or a negative
    # one makes the comparison with the budget pass for every job.
    function JobSpec(name, partition, nodes, time_limit, script, env, args)
        nodes >= 1 || throw(ArgumentError("JobSpec $name: nodes must be >= 1, got $nodes"))
        (isfinite(time_limit) && time_limit > 0) || throw(
            ArgumentError(
                "JobSpec $name: time_limit must be positive and finite, got $time_limit"
            ),
        )
        return new(name, partition, nodes, time_limit, script, env, args)
    end
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
    # `time_limit` may be `Inf` (no limit: it commits without bound); nothing may be NaN or
    # negative.
    function JobState(id, name, partition, state, nodes, time_limit, elapsed)
        bad(msg) = throw(ArgumentError("JobState $id: $msg"))
        nodes >= 0 || bad("nodes must be >= 0, got $nodes")
        (!isnan(time_limit) && time_limit >= 0) ||
            bad("time_limit must be >= 0 and not NaN, got $time_limit")
        (isfinite(elapsed) && elapsed >= 0) ||
            bad("elapsed must be finite and >= 0, got $elapsed")
        return new(id, name, partition, state, nodes, time_limit, elapsed)
    end
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

# `D-HH:MM:SS`, `HH:MM:SS`, `MM:SS`, `SS` as Slurm prints them; `Inf` for UNLIMITED; `0` for
# the placeholders of a job that has not started (`N/A`, empty). `nothing` for anything else it
# cannot read: an unreadable time is not zero time.
function _slurm_time(s::AbstractString)::Union{Float64,Nothing}
    s = strip(s)
    (isempty(s) || s == "N/A") && return 0.0
    s == "UNLIMITED" && return Inf
    days = 0.0
    if occursin('-', s)
        d, s = split(s, '-'; limit=2)
        dv = tryparse(Float64, d)
        dv === nothing && return nothing
        days = dv
    end
    secs = 0.0
    for p in split(s, ':')
        v = tryparse(Float64, p)
        v === nothing && return nothing
        secs = 60secs + v
    end
    return 86400days + secs
end

# The lenient form: what cannot be read is 0. For display only; nothing that is charged uses it.
_slurm_seconds(s::AbstractString)::Float64 = something(_slurm_time(s), 0.0)

# Seconds as `sbatch -t` takes them: whole minutes, rounded up.
_slurm_minutes(secs::Real) = string(max(1, ceil(Int, secs / 60)))

# Run a command with a bound and return its stdout, or `nothing` if it failed or timed out.
function _run_command(cmd::Cmd; timeout::Real=60.0)::Union{String,Nothing}
    out = tempname()
    err = tempname()
    name = String(first(cmd.exec))
    _COMMAND_FAILURE[] = ""
    proc = try
        run(pipeline(cmd; stdout=out, stderr=err); wait=false)
    catch e
        e isa InterruptException && rethrow()
        _COMMAND_FAILURE[] = "$name could not be started: $(_short_err(e))"
        rm(err; force=true)
        return nothing
    end
    try
        if timedwait(() -> !process_running(proc), Float64(timeout); pollint=0.05) !== :ok
            kill(proc, Base.SIGKILL)
            _COMMAND_FAILURE[] = "$name did not answer within $(timeout) s"
            return nothing
        end
        success(proc) && return read(out, String)
        said = try
            strip(read(err, String))
        catch
            ""
        end
        _COMMAND_FAILURE[] =
            "$name exited with code $(proc.exitcode)" *
            (isempty(said) ? "" : ": " * first(said, 500))
        return nothing
    finally
        rm(out; force=true)
        rm(err; force=true)
    end
end

# Why the last command run through `_run_command` gave `nothing`: could not be started, timed
# out, or its exit code and what it wrote to stderr. The three used to be one silent `nothing`,
# so the error was "sbatch failed" with no reason.
const _COMMAND_FAILURE = Ref("")

# Run one scheduler command through the scheduler's `run`. The reason is cleared first: with a
# `run=` hook that is not `_run_command`, a failure used to carry the reason of whatever command
# had failed before it.
_run(s, cmd::Cmd) = (_COMMAND_FAILURE[]=""; s.run(cmd))
_why_failed() = isempty(_COMMAND_FAILURE[]) ? "" : " (" * _COMMAND_FAILURE[] * ")"

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
    out = _run(s, cmd)
    out === nothing &&
        error("sbatch failed for job $(spec.name) on $(spec.partition)" * _why_failed())
    id = strip(first(split(strip(out), ';')))
    isempty(id) && error("sbatch printed no job id for $(spec.name)")
    return String(id)
end

cancel(s::SlurmScheduler, id::AbstractString) = _run(s, `scancel $id`) !== nothing

function job_states(s::SlurmScheduler)::Vector{JobState}
    # The name last, and split with a limit: a job name may contain the separator.
    out = _run(s, `squeue -h -u $(s.user) -o "%i|%P|%T|%D|%l|%M|%j"`)
    out === nothing &&
        error("squeue failed: the jobs that exist are not known" * _why_failed())
    jobs = JobState[]
    for line in split(out, '\n'; keepempty=false)
        f = split(strip(line, ['"', ' ']), '|'; limit=7)
        # A line that cannot be read is a job that would go uncounted: the whole answer is
        # refused rather than trusted in part.
        length(f) == 7 || error("squeue: cannot read the line $(repr(line))")
        nodes = tryparse(Int, f[4])
        # A limit Slurm prints as a word (`NOT_SET`, `INVALID`, `Partition_Limit`) is "not
        # known": taken as no limit, which commits without bound if the job is ours and costs
        # nothing if it is not. Refusing the whole answer over it stopped every round for as
        # long as ANY job of the user had such a limit.
        limit = something(_slurm_time(f[5]), Inf)
        elapsed = _slurm_time(f[6])
        (nodes === nothing || elapsed === nothing) &&
            error("squeue: cannot read nodes or elapsed time in $(repr(line))")
        state = if f[3] == "RUNNING"
            :running
        elseif f[3] == "PENDING"
            :pending
        else
            # CONFIGURING, COMPLETING, SUSPENDED, …: listed, so it exists and is charged.
            :other
        end
        push!(
            jobs,
            JobState(
                String(f[1]), String(f[7]), String(f[2]), state, nodes, limit, elapsed
            ),
        )
    end
    return jobs
end

# States `sacct` gives a job that is over, and ones it gives a job that still exists.
const _SACCT_OVER = (
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "OUT_OF_MEMORY",
    "NODE_FAIL",
    "PREEMPTED",
    "BOOT_FAIL",
    "DEADLINE",
)
const _SACCT_LIVE = (
    "PENDING", "RUNNING", "SUSPENDED", "COMPLETING", "CONFIGURING", "REQUEUED", "RESIZING"
)

# Asked of the accounting, for a job the queue no longer lists. `nothing` whenever the answer is
# not one of the known states: accounting off, the job not in it yet, the command failing.
# (Read against stubs only: this has not been run on a cluster.)
function job_gone(s::SlurmScheduler, id::AbstractString)
    out = _run(s, `sacct -n -X -P -j $id -o State`)
    # The command failing is not "no evidence": it is said (the caller logs it), so that an
    # accounting that is off or down is known to be why a job's end is seen late.
    out === nothing && error("sacct failed for job $id" * _why_failed())
    lines = split(out, '\n'; keepempty=false)
    isempty(lines) && return nothing
    state = first(split(strip(first(lines))))          # "CANCELLED by 1234" -> CANCELLED
    state in _SACCT_OVER && return true
    state in _SACCT_LIVE && return false
    return nothing
end

function remaining_time(s::SlurmScheduler, id::AbstractString)
    out = _run(s, `squeue -h -j $id -o %L`)
    (out === nothing || isempty(strip(out))) && return nothing
    # `nothing` for a time that cannot be read (`NOT_SET`, `INVALID`), as documented: not 0.
    return _slurm_time(out)
end

function shrink(s::SlurmScheduler, id::AbstractString, nodes::Integer)
    return _run(s, `scontrol update JobId=$id NumNodes=$nodes`) !== nothing
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

# The mock's accounting: a job it issued (ids count up from 1000) and no longer lists has ended.
function job_gone(s::MockScheduler, id::AbstractString)
    any(j -> j.id == id, s.jobs) && return false
    n = tryparse(Int, id)
    return (n !== nothing && 1000 < n <= s.next) ? true : nothing
end

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
struct PartitionPolicy
    name::String
    nodes::Int
    time_limit::Float64
    script::String
    profile::Union{String,Nothing}
    max_jobs::Int
    slots_per_node::Int
    key_time::Union{Float64,Nothing}
    env::Dict{String,String}
    # The checks are here, in the only way to make one: built positionally, a policy used to
    # skip them.
    function PartitionPolicy(
        name, nodes, time_limit, script, profile, max_jobs, slots_per_node, key_time, env
    )
        bad(msg) = throw(ArgumentError("PartitionPolicy $name: $msg"))
        # Each of these, wrong, makes a job's cost zero, negative or NaN — and a comparison
        # with the budget that every job passes.
        nodes >= 1 || bad("nodes must be >= 1, got $nodes")
        (isfinite(time_limit) && time_limit > 0) ||
            bad("time_limit must be positive and finite, got $time_limit")
        slots_per_node >= 1 || bad("slots_per_node must be >= 1, got $slots_per_node")
        max_jobs >= 0 || bad("max_jobs must be >= 0, got $max_jobs")
        (key_time === nothing || (isfinite(key_time) && key_time > 0)) ||
            bad("key_time must be positive and finite, got $key_time")
        return new(
            String(name),
            Int(nodes),
            Float64(time_limit),
            String(script),
            profile === nothing ? nothing : String(profile),
            Int(max_jobs),
            Int(slots_per_node),
            key_time === nothing ? nothing : Float64(key_time),
            Dict{String,String}(String(k) => String(v) for (k, v) in env),
        )
    end
end

function PartitionPolicy(;
    name::AbstractString,
    nodes::Integer,
    time_limit::Real,
    script::AbstractString,
    profile::Union{AbstractString,Nothing}=nothing,
    max_jobs::Integer=1,
    slots_per_node::Integer=1,
    key_time::Union{Real,Nothing}=nothing,
    env::AbstractDict=Dict{String,String}(),
)
    return PartitionPolicy(
        name, nodes, time_limit, script, profile, max_jobs, slots_per_node, key_time, env
    )
end

"""
    JobPolicy(; name, partitions, budget_node_hours, max_jobs=typemax(Int), dry_run=true,
              default_key_time=600.0)

The rules a controller submits under. `name` prefixes every job it submits and is how it
recognises its own. `budget_node_hours` is hard: a submission that would take used plus
committed node-hours past it is refused. `dry_run` (the default) decides and logs but submits
nothing.
"""
struct JobPolicy
    name::String
    partitions::Vector{PartitionPolicy}
    budget_node_hours::Float64
    max_jobs::Int
    dry_run::Bool
    default_key_time::Float64
    function JobPolicy(
        name, partitions, budget_node_hours, max_jobs, dry_run, default_key_time
    )
        bad(msg) = throw(ArgumentError("JobPolicy $name: $msg"))
        isempty(name) &&
            bad("name must not be empty: it is how the controller knows its jobs")
        # NaN would make `spent + job > budget` false for every job.
        (isfinite(budget_node_hours) && budget_node_hours >= 0) ||
            bad("budget_node_hours must be finite and >= 0, got $budget_node_hours")
        max_jobs >= 0 || bad("max_jobs must be >= 0, got $max_jobs")
        (isfinite(default_key_time) && default_key_time > 0) ||
            bad("default_key_time must be positive and finite, got $default_key_time")
        allunique(p.name for p in partitions) || bad("a partition is named twice")
        # A copy: the caller's vector is not this policy's.
        return new(
            String(name),
            collect(PartitionPolicy, partitions),
            Float64(budget_node_hours),
            Int(max_jobs),
            dry_run,
            Float64(default_key_time),
        )
    end
end

function JobPolicy(;
    name::AbstractString,
    partitions::AbstractVector{PartitionPolicy},
    budget_node_hours::Real,
    max_jobs::Integer=typemax(Int),
    dry_run::Bool=true,
    default_key_time::Real=600.0,
)
    return JobPolicy(
        name, partitions, budget_node_hours, max_jobs, dry_run, default_key_time
    )
end

# The names this policy gives its jobs: one per partition. Exact, so a policy named `ft` does
# not claim the jobs of one named `ft2`.
_job_name(policy::JobPolicy, p::PartitionPolicy) = string(policy.name, "-", p.name)
_job_names(policy::JobPolicy) = Set(_job_name(policy, p) for p in policy.partitions)

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

Ledger(path::AbstractString) = Ledger(String(path), _read_ledger(path))

# The rows of a ledger file, checked. A number that is missing, NaN or negative makes the
# account a number the budget check passes for every job, so such a file is an error, said with
# the row — not a ledger to decide on.
function _read_ledger(path::AbstractString)
    jobs = Dict{String,Dict{String,Any}}()
    isfile(path) || return jobs
    raw = JSON3.read(read(path, String), Dict{String,Any})
    for (id, j) in get(raw, "jobs", Dict{String,Any}())
        row = Dict{String,Any}(j)
        _check_ledger_row(String(id), row)
        jobs[String(id)] = row
    end
    return jobs
end

function _check_ledger_row(id::AbstractString, j::AbstractDict)
    bad(msg) = throw(ArgumentError("ledger row $id: $msg"))
    for f in ("name", "partition", "nodes", "time_limit", "elapsed", "state", "ended")
        haskey(j, f) || bad("no `$f`")
    end
    n = j["nodes"]
    (n isa Real && isfinite(n) && n >= 0) ||
        bad("nodes must be a number >= 0, got $(repr(n))")
    e = j["elapsed"]
    (e isa Real && isfinite(e) && e >= 0) ||
        bad("elapsed must be finite and >= 0, got $(repr(e))")
    t = j["time_limit"]
    # JSON has no Inf: a job without a limit is written as `null`.
    t === nothing && (j["time_limit"]=Inf; t=Inf)
    (t isa Real && !isnan(t) && t >= 0) ||
        bad("time_limit must be >= 0 and not NaN, got $(repr(t))")
    j["ended"] isa Bool || bad("ended must be true or false, got $(repr(j["ended"]))")
    j["state"] isa AbstractString || bad("state must be a string, got $(repr(j["state"]))")
    # One fact in two fields: they are made to agree where the row enters.
    j["ended"] && (j["state"] = "ended")
    (j["state"] == "ended" && !j["ended"]) && (j["ended"] = true)
    return nothing
end

function save_ledger(l::Ledger)
    _check_ledger_held(l)
    rows = Dict(
        id => Dict{String,Any}(
            k => (v isa AbstractFloat && isinf(v) ? nothing : v) for (k, v) in j
        ) for (id, j) in l.jobs
    )
    atomic_write(io -> JSON3.write(io, Dict("jobs" => rows)), l.path)
    return l.path
end

# Whether the ledger's file was there when it was last read: a missing file reads as "nothing
# used", which a controller must say rather than assume.
_ledger_exists(l::Ledger) = isfile(l.path)

# Take what is on disk now. A controller is not the only writer: a job's last act and a
# login-node loop share one ledger.
function reload_ledger!(l::Ledger)
    fresh = _read_ledger(l.path)
    empty!(l.jobs)
    merge!(l.jobs, fresh)
    return l
end

# How long a ledger lock may go unrefreshed before it is taken as left behind by a controller
# that died. Ten minutes: its holder refreshes it every minute, and a round's slowest calls
# (`squeue`, `sacct`, `sbatch`) are each bounded at one.
const _LEDGER_LOCK_STALE = Ref(600.0)

"""
    with_ledger(f, ledger; wait=120.0)

Run `f()` holding the ledger's lock (a directory beside the file: `mkdir` is atomic on NFS), with
the ledger re-read from disk first. Two controllers on one ledger then decide one after the
other, each on what the other wrote, instead of overwriting each other's rows. Throws if the lock
cannot be had within `wait` seconds.
"""
function with_ledger(f, l::Ledger; wait::Real=120.0)
    lockdir = l.path * ".lock"
    mkpath(dirname(l.path))
    token = string(gethostname(), ":", getpid(), ":", string(rand(UInt64); base=16))
    t0 = time()
    watched = Ref(-1.0)                 # the lock's timestamp as last read, and since when
    watched_since = Ref(time())
    while true
        got = try
            mkdir(lockdir)
            true
        catch e
            e isa InterruptException && rethrow()
            isdir(lockdir) || rethrow()
            false
        end
        if got
            try
                write(joinpath(lockdir, "owner"), token)
            catch
                # A lock with no owner would hold everyone off for as long as a lock may go
                # unrefreshed: not left behind.
                rm(lockdir; force=true, recursive=true)
                rethrow()
            end
            break
        end
        # Left behind by a controller that died holding it? Its holder refreshes the lock while
        # it lives, so a dead one stops changing. Judged on what THIS process has watched, not
        # on the file's timestamp against this host's clock alone: with two hosts' clocks a
        # waiter that is ahead saw every lock as stale and took it each time. Stale is: old by
        # the timestamp AND seen not to change for longer than its holder would leave it.
        touched = _lock_touched(lockdir)
        if touched != watched[]
            watched[] = touched
            watched_since[] = time()
        end
        age = time() - touched
        unchanged = time() - watched_since[]
        if age > _LEDGER_LOCK_STALE[] && unchanged > 1.5 * _LEDGER_LOCK_STALE[] / 10
            aside = string(lockdir, ".stale.", string(rand(UInt32); base=16))
            try
                mv(lockdir, aside)
                rm(aside; force=true, recursive=true)
            catch e
                e isa InterruptException && rethrow()
            end
            continue
        end
        time() - t0 > wait && error(
            "the ledger $(l.path) is locked by another controller " *
            "($(round(Int, age)) s); nothing decided",
        )
        sleep(0.2)
    end
    # While it is held it is kept fresh, so that a round that is slow (an `sacct` per absent
    # job, a campaign scan, an `sbatch` per decision) is not taken for one that died.
    alive = Ref(true)
    keeper = @async while alive[]
        try
            # Only while it is still ours: a holder that lost the lock must not keep another's
            # fresh, nor put an owner file back.
            _lock_token(lockdir) == token && touch(joinpath(lockdir, "owner"))
        catch
        end
        timedwait(() -> !alive[], _LEDGER_LOCK_STALE[] / 10; pollint=0.05)
    end
    _LEDGER_HELD[l] = (lockdir, token)
    try
        reload_ledger!(l)
        return f()
    finally
        alive[] = false
        delete!(_LEDGER_HELD, l)
        # Only its owner removes it: if it was taken from us, the lock there is someone else's.
        _lock_token(lockdir) == token && rm(lockdir; force=true, recursive=true)
    end
end

# Ledgers this process holds the lock of: the ledger object => (lock directory, token). By
# object, not by path: two controllers in one process are two holders.
const _LEDGER_HELD = IdDict{Any,Tuple{String,String}}()

function _lock_token(lockdir::AbstractString)
    return try
        read(joinpath(lockdir, "owner"), String)
    catch
        nothing
    end
end

# When the lock was last refreshed: the owner file if it is there, else the directory.
function _lock_touched(lockdir::AbstractString)
    f = joinpath(lockdir, "owner")
    return try
        isfile(f) ? mtime(f) : mtime(lockdir)
    catch
        time()
    end
end

# Still ours? Asked before every write made under the lock: a controller that lost it (it was
# taken as stale while this one was stuck) must not write its copy over what the other decided.
function _check_ledger_held(l::Ledger)
    held = get(_LEDGER_HELD, l, nothing)
    held === nothing && return nothing             # written outside a lock (a test, a tool)
    _lock_token(held[1]) == held[2] || error(
        "the lock on the ledger $(l.path) was taken by another controller; not written"
    )
    return nothing
end

# How many polls in a row a job has to be absent from the scheduler's answer before the ledger
# takes it as ended. One absence is not evidence: a job between states, a controller that
# answered for part of the queue.
const _ENDED_AFTER_MISSING = 3

function _ledger_row(spec::JobSpec, state::AbstractString, now::Real)
    return Dict{String,Any}(
        "name" => spec.name,
        "partition" => spec.partition,
        "nodes" => spec.nodes,
        "time_limit" => spec.time_limit,
        "submitted" => Float64(now),
        "elapsed" => 0.0,
        "state" => String(state),
        "ended" => false,
        "missing" => 0,
        "last_seen" => Float64(now),
    )
end

function record_submit!(l::Ledger, id::AbstractString, spec::JobSpec; now::Real=time())
    l.jobs[String(id)] = _ledger_row(spec, "pending", now)
    return nothing
end

"""
    record_intent!(ledger, spec) -> String

Write a ledger row for a submission that is ABOUT to be made, save the ledger, and return the
row's provisional id. The row commits the job's node-hours from this moment. If `sbatch` then
succeeds, [`confirm_submit!`](@ref) gives the row its real id; if it fails or the controller dies
in between, the row stays — committed, and holding its place under `max_jobs` — and is either
matched to the job by name on a later poll or dropped once it has gone unlisted for
$(_ENDED_AFTER_MISSING) polls AND ten minutes. A job that was queued but never recorded is the one
a budget cannot see.
"""
function record_intent!(l::Ledger, spec::JobSpec; now::Real=time())
    tmp = string("submitting-", round(Int, now * 1000), "-", string(rand(UInt32); base=16))
    l.jobs[tmp] = _ledger_row(spec, "submitting", now)
    save_ledger(l)
    return tmp
end

"""
    confirm_submit!(ledger, provisional, id)

The submission recorded as `provisional` got the scheduler's `id`. The ledger is saved.
"""
function confirm_submit!(l::Ledger, tmp::AbstractString, id::AbstractString)
    row = pop!(l.jobs, String(tmp))
    row["state"] = "pending"
    old = get(l.jobs, String(id), nothing)
    if old !== nothing
        # The id is already on record (adopted from a poll in between): one row, with the
        # larger of what the two know.
        row["elapsed"] = max(Float64(row["elapsed"]), Float64(old["elapsed"]))
        row["state"] = old["state"]
    end
    l.jobs[String(id)] = row
    save_ledger(l)
    return nothing
end

# For how long a job has to have been absent, beside the number of polls, before absence is taken
# as its end: three polls a second apart are one moment, not three.
# On how many polls the accounting has to call a job over before that ends it.
const _GONE_CONFIRM = 2
const _ENDED_MIN_ABSENT = Ref(120.0)
# The same, for answers that list no job of the ledger at all: half an hour — long enough that
# a wrapper or a wrong cluster answering for a few polls ends nothing, short enough that a
# controller is not stuck behind the last job of a partition for its whole time limit.
const _ENDED_MIN_ABSENT_ALONE = Ref(1800.0)
# How long a submission may go unlisted before it is taken as not made.
const _SUBMIT_UNSEEN = Ref(600.0)

"""
    job_gone(scheduler, id) -> Union{Bool,Nothing}

Positive evidence about a job the queue no longer lists: `true` it has ended, `false` it exists,
`nothing` the scheduler cannot say (the default).
"""
job_gone(::Scheduler, ::AbstractString) = nothing

"""
    observe!(ledger, states; now=time(), names=(), gone=id -> nothing) -> Vector{NamedTuple}

Bring the ledger up to what the scheduler says, and return what changed
(`(; id, what, evidence, …)`, `what` being `:ended` or `:adopted`), for the caller to log.

- A job the scheduler lists — in ANY state — exists: its elapsed time and state are updated.
- A listed job named as one of ours (`names`) that the ledger does not have is ADOPTED, at its
  limit: a job the ledger does not know is one the budget does not see.
- A job it does not list is ended only on evidence:
  - `gone(id) === true` on two polls in a row (the scheduler's accounting says so), or
  - it has been absent for $(_ENDED_AFTER_MISSING) polls over `_ENDED_MIN_ABSENT` seconds (two
    minutes) from answers that list at least one other job the ledger knows, or
  - it has been absent for $(_ENDED_AFTER_MISSING) polls over `_ENDED_MIN_ABSENT_ALONE` seconds
    (half an hour) from answers that list NONE of the ledger's jobs — empty, another cluster, a
    filter — or while the accounting still calls it live. This is weak evidence, marked `weak`
    in what is returned: it is what lets the end of the last live job be seen at all, and it is
    also what the wrong cluster gives. (Three such answers in a row, with no time required,
    used to empty the ledger.) Or
  - the clock says it cannot be running: it was seen running and its time limit has passed.
- A job ended by absence is billed for what it can have run since it was last seen (up to its
  time limit). A job that reappears is live again.
- A row still `submitting` (the controller did not learn the id) takes the id of a listed job
  with its name that the ledger does not have yet.
"""
function observe!(
    l::Ledger,
    states::AbstractVector{JobState};
    now::Real=time(),
    names=(),
    gone=id -> nothing,
)
    changes = NamedTuple[]
    by = Dict(j.id => j for j in states)
    unclaimed = [j for j in states if !haskey(l.jobs, j.id)]
    trusted = any(j -> haskey(l.jobs, j.id), states)
    for id in collect(keys(l.jobs))
        j = l.jobs[id]
        s = get(by, id, nothing)
        if s === nothing && j["state"] == "submitting"
            k = findfirst(
                u -> u.name == j["name"] && u.partition == j["partition"], unclaimed
            )
            if k !== nothing
                s = unclaimed[k]
                deleteat!(unclaimed, k)
                delete!(l.jobs, id)
                l.jobs[s.id] = j
                id = s.id
            end
        end
        if s !== nothing
            j["elapsed"] = max(Float64(j["elapsed"]), s.elapsed)
            j["state"] = String(s.state)
            j["missing"] = 0
            j["last_seen"] = Float64(now)
            j["ended"] = false
            delete!(j, "absent_since")
            continue
        end
        j["ended"] === true && continue
        since = Float64(now) - Float64(get(j, "last_seen", now))
        evidence = nothing
        weak = false
        if j["state"] == "submitting"
            # Never listed. A queued job shows within seconds, so one that has not in
            # `_SUBMIT_UNSEEN` was not taken; if it does turn up later it is adopted by name.
            j["missing"] = Int(get(j, "missing", 0)) + 1
            if j["missing"] >= _ENDED_AFTER_MISSING && since >= _SUBMIT_UNSEEN[]
                j["ended"] = true
                push!(
                    changes,
                    (;
                        id=id,
                        what=:ended,
                        weak=false,
                        evidence="submitted $(round(Int, since)) s ago and never listed",
                        was="submitting",
                        polls_missed=j["missing"],
                        seconds_billed=0.0,
                    ),
                )
                j["state"] = "ended"
            end
            continue
        end
        g = gone(id)
        # One answer of the accounting is not acted on by itself: a job id that was reused, or
        # another cluster's accounting, is enough for one. It has to say so on two polls.
        j["gone_seen"] = g === true ? Int(get(j, "gone_seen", 0)) + 1 : 0
        if g === true && j["gone_seen"] >= _GONE_CONFIRM
            evidence = "the scheduler's accounting says it ended (on $(j["gone_seen"]) polls)"
        elseif g === true
            continue                                 # once: asked again on the next poll
        else
            # The scheduler answered and does not list it. An answer that lists other jobs of
            # the ledger can be read for absence after a short while. One that lists NONE of
            # them — the usual case with one job per partition, and also what the wrong
            # cluster or a filter gives — counts too, but only over a long wall time: without
            # that the end of the last live job could not be seen at all, and a pending job
            # that was cancelled blocked the controller for ever.
            j["missing"] = Int(get(j, "missing", 0)) + 1
            get!(j, "absent_since", Float64(now))
            absent = Float64(now) - Float64(j["absent_since"])
            # The accounting says it is live while the queue does not list it: the queue is
            # believed only over the long wall time, as an answer listing none of ours is. (It
            # used to be believed never: a runaway record kept the job live for ever, every
            # round refused.)
            sure = trusted && g !== false
            need = sure ? _ENDED_MIN_ABSENT[] : _ENDED_MIN_ABSENT_ALONE[]
            if j["missing"] >= _ENDED_AFTER_MISSING && absent >= need
                weak = !sure
                evidence = if g === false
                    "absent from the queue on $(j["missing"]) answers over " *
                    "$(round(Int, absent)) s, although the accounting still lists it as live"
                elseif trusted
                    "absent from $(j["missing"]) answers that listed other jobs of the " *
                    "ledger, over $(round(Int, absent)) s"
                else
                    "absent from $(j["missing"]) answers over $(round(Int, absent)) s " *
                    "(none of them listed any job of the ledger)"
                end
            end
            # Whatever the answers are worth: a job seen running cannot outlive its limit.
            left = Float64(j["time_limit"]) - Float64(j["elapsed"])
            if evidence === nothing && j["state"] == "running" && since > left + 60
                evidence = "last seen running $(round(Int, since)) s ago with $(round(Int, max(left, 0.0))) s of its limit left"
            end
        end
        evidence === nothing && continue
        j["ended"] = true
        billed = 0.0
        if j["state"] != "submitting"
            # It ran, at most, from when it was last seen until now.
            before = Float64(j["elapsed"])
            j["elapsed"] = min(Float64(j["time_limit"]), before + max(since, 0.0))
            isfinite(j["elapsed"]) || (j["elapsed"] = before + max(since, 0.0))
            billed = j["elapsed"]
        end
        push!(
            changes,
            (;
                id=id,
                what=:ended,
                evidence=evidence,
                # Ended on what the wrong cluster or a filter would also give: to be looked at.
                weak=weak,
                was=String(j["state"]),
                polls_missed=Int(get(j, "missing", 0)),
                seconds_billed=billed,
            ),
        )
        j["state"] = "ended"
    end
    # Ours by name, listed, and in no row: adopted at its limit.
    for u in unclaimed
        u.name in names || continue
        l.jobs[u.id] = Dict{String,Any}(
            "name" => u.name,
            "partition" => u.partition,
            "nodes" => u.nodes,
            "time_limit" => u.time_limit,
            "submitted" => Float64(now),
            "elapsed" => u.elapsed,
            "state" => String(u.state),
            "ended" => false,
            "missing" => 0,
            "last_seen" => Float64(now),
            "adopted" => true,
        )
        push!(
            changes,
            (;
                id=u.id,
                what=:adopted,
                weak=false,
                evidence="listed under the policy's name and in no ledger row",
                was=String(u.state),
                polls_missed=0,
                seconds_billed=u.elapsed,
            ),
        )
    end
    return changes
end

"""
    node_hours(ledger) -> NamedTuple

`(; used, committed, by_partition)`: node-hours the ledger's jobs have run, node-hours its live
jobs can still run (to their time limits), and `partition => (; used, committed)`. A job with no
time limit commits `Inf`: a budget cannot hold an unlimited job.
"""
function node_hours(l::Ledger)
    used = committed = 0.0
    by = Dict{String,Tuple{Float64,Float64}}()
    for j in values(l.jobs)
        u = Float64(j["nodes"]) * Float64(j["elapsed"]) / 3600
        c = if j["ended"] === true || j["nodes"] == 0     # no nodes: nothing, not 0 * Inf
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

# Does the ledger hold a job the scheduler should still know about?
_ledger_live(l::Ledger) = any(j -> j["ended"] !== true, values(l.jobs))

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
    names = _job_names(policy)
    # Every listed job of ours exists and holds its place, whatever state it is in.
    mine = [j for j in jobs if haskey(ledger.jobs, j.id) || j.name in names]
    nh = node_hours(ledger)
    spent = nh.used + nh.committed
    # A row still `submitting` is a job that may be queued and not listed yet (an `sbatch` that
    # timed out after it was taken): it holds a place too.
    unlisted = [
        j for j in values(ledger.jobs) if j["state"] == "submitting" && j["ended"] !== true
    ]
    live_total = length(mine) + length(unlisted)
    for p in policy.partitions
        w = work(p.profile)
        live = [j for j in mine if j.partition == p.name]
        pending_here = count(j -> j["partition"] == p.name, unlisted)
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
            # (no nodes: nothing, not `0 * Inf`)
            j -> if j.nodes == 0
                0.0
            else
                j.nodes * p.slots_per_node * max(j.time_limit - j.elapsed, 0.0)
            end,
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
        room = min(p.max_jobs - length(live) - pending_here, policy.max_jobs - live_total)
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
                name=_job_name(policy, p),
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
id), and the ledger is saved under its lock. A `dry_run` round writes no ledger; it does write
its events, marked `dry_run`. A round that could not ask the scheduler, could not trust its
answer, or could not use the ledger returns `:refuse` for every partition.

Call it from a job that is ending to resubmit only if work remains, or in
[`controller_loop!`](@ref) to keep a campaign supplied.
"""
function manage!(ctl::JobController, work)::Vector{Decision}
    # A dry run looks: it decides on a copy of what is on disk and writes no ledger. (It does
    # log what it decided, marked `dry_run`.)
    if ctl.policy.dry_run
        states = try
            job_states(ctl.scheduler)
        catch e
            e isa InterruptException && rethrow()
            return _refuse_all(ctl, "the scheduler could not be asked: $(_short_err(e))")
        end
        seen = try
            Ledger(ctl.ledger.path, deepcopy(_read_ledger(ctl.ledger.path)))
        catch e
            e isa InterruptException && rethrow()
            return _refuse_all(ctl, "the ledger cannot be read: $(_short_err(e))")
        end
        _observe_and_say!(ctl, seen, states; dry=true)
        empty!(ctl.ledger.jobs)
        merge!(ctl.ledger.jobs, seen.jobs)             # what the caller prints is what was seen
        # The same guard as a real round: a look must not print `submit` where `--submit`
        # would refuse.
        why = _untrusted(seen, states)
        why === nothing || return _refuse_all(ctl, why)
        decisions = decide(ctl.policy, work, states, seen)
        foreach(d -> _say_decision(ctl, d), decisions)
        return decisions
    end
    # Under the ledger's lock, on what is on disk now: a job's last act and a login-node loop
    # are two controllers on one ledger. The scheduler is asked INSIDE the lock: asked before
    # it, a job the other controller submitted while this one waited was in the ledger and not
    # in the answer, and `max_jobs` could be passed. A ledger that cannot be locked or read is a
    # refusal; an error inside the round is the round's own and is raised.
    entered = Ref(false)
    return try
        with_ledger(ctl.ledger) do
            states = try
                job_states(ctl.scheduler)
            catch e
                e isa InterruptException && rethrow()
                return _refuse_all(
                    ctl, "the scheduler could not be asked: $(_short_err(e))"
                )
            end
            entered[] = true
            return _manage_locked!(ctl, work, states)
        end
    catch e
        (e isa InterruptException || entered[]) && rethrow()
        _refuse_all(ctl, "the ledger could not be used: $(_short_err(e))")
    end
end

"""
    forget_job!(controller, id) -> Bool

Mark the ledger's job `id` as ended, by hand: for a job this controller cannot see the end of (a
pending job that was cancelled, on a cluster without accounting). It is billed for what the
ledger last knew it ran. Logged as `job_ended` with `evidence = "forgotten by hand"`. Returns
whether there was such a live job.
"""
function forget_job!(ctl::JobController, id::AbstractString)
    return with_ledger(ctl.ledger) do
        j = get(ctl.ledger.jobs, String(id), nothing)
        (j === nothing || j["ended"] === true) && return false
        was = String(j["state"])
        j["ended"] = true
        j["state"] = "ended"
        save_ledger(ctl.ledger)
        log_event(
            ctl.log,
            :job_ended;
            level=:warn,
            id=String(id),
            evidence="forgotten by hand",
            was=was,
            polls_missed=Int(get(j, "missing", 0)),
            seconds_billed=Float64(j["elapsed"]),
            dry_run=false,
        )
        return true
    end
end

# Why an answer is not one to submit on, or `nothing`: it lists none of the jobs the ledger has
# live. They are there for all this controller knows; they end on evidence (see `observe!`),
# not on how often the same answer is repeated.
function _untrusted(l::Ledger, states)
    _ledger_live(l) || return nothing
    any(j -> haskey(l.jobs, j.id) && l.jobs[j.id]["ended"] !== true, states) &&
        return nothing
    n = count(j -> j["ended"] !== true, values(l.jobs))
    return "the scheduler lists none of the $n job(s) the ledger has live; not trusted, " *
           "nothing submitted this round"
end

function _say_decision(ctl::JobController, d::Decision)
    return log_event(
        ctl.log,
        :job_decision;
        level=d.action === :refuse ? :warn : :info,
        action=String(d.action),
        partition=d.partition,
        reason=d.reason,
        node_hours=d.node_hours,
        dry_run=ctl.policy.dry_run,
    )
end

# Bring `l` up to the scheduler's answer and log what that changed.
function _observe_and_say!(ctl::JobController, l::Ledger, states; dry::Bool)
    said_gone = Ref(false)
    changes = observe!(
        l,
        states;
        names=_job_names(ctl.policy),
        gone=id -> try
            job_gone(ctl.scheduler, id)
        catch e
            e isa InterruptException && rethrow()
            # The accounting could not be asked: no evidence either way, and said — once a
            # round, not once per absent job.
            if !said_gone[]
                said_gone[] = true
                log_event(
                    ctl.log,
                    :job_gone_failed;
                    level=:warn,
                    id=id,
                    err=_short_err(e),
                    dry_run=dry,
                )
            end
            nothing
        end,
    )
    for c in changes
        log_event(
            ctl.log,
            c.what === :ended ? :job_ended : :job_adopted;
            # Ended on weak evidence is a warning: the controller is about to submit on top of
            # a job that may still be there.
            level=(c.what === :adopted || c.weak) ? :warn : :info,
            id=c.id,
            evidence=c.evidence,
            weak=c.weak,
            was=c.was,
            polls_missed=c.polls_missed,
            seconds_billed=c.seconds_billed,
            dry_run=dry,
        )
    end
    return changes
end

function _manage_locked!(ctl::JobController, work, states)::Vector{Decision}
    l = ctl.ledger
    if !_ledger_exists(l) && isempty(l.jobs)
        # "Nothing used so far" is an assumption when the file is not there; said once, when
        # the file is first written.
        log_event(ctl.log, :ledger_new; level=:warn, path=l.path)
    end
    _observe_and_say!(ctl, l, states; dry=false)
    save_ledger(l)
    why = _untrusted(l, states)
    why === nothing || return _refuse_all(ctl, why)
    decisions = decide(ctl.policy, work, states, l)
    for (n, d) in enumerate(decisions)
        _say_decision(ctl, d)
        d.action === :submit || continue
        # On record BEFORE the scheduler is called: a job that is queued and not in the ledger
        # is one the budget does not see.
        tmp = record_intent!(l, d.spec)
        id = try
            submit(ctl.scheduler, d.spec)
        catch e
            e isa InterruptException && rethrow()
            # It may have been queued all the same (a timeout after sbatch took it). The row
            # stays, committed and holding its place, until a poll finds the job or it has
            # gone unlisted long enough.
            log_event(
                ctl.log,
                :job_submit_failed;
                level=:warn,
                partition=d.partition,
                err=_short_err(e),
                ledger_row=tmp,
            )
            # What the caller is told is what happened: not a submission.
            decisions[n] = Decision(
                :refuse,
                d.partition,
                "the submission failed: $(_short_err(e)) (its row stays committed until " *
                "the job is found or has gone unlisted)",
                d.spec,
                d.node_hours,
            )
            continue
        end
        confirm_submit!(l, tmp, id)
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
    save_ledger(l)
    return decisions
end

# Every partition refused, for one reason, said in the log.
function _refuse_all(ctl::JobController, why::AbstractString)
    ds = [
        Decision(:refuse, p.name, String(why), nothing, 0.0) for p in ctl.policy.partitions
    ]
    for d in ds
        log_event(
            ctl.log,
            :job_decision;
            level=:warn,
            action="refuse",
            partition=d.partition,
            reason=d.reason,
            node_hours=0.0,
            dry_run=ctl.policy.dry_run,
        )
    end
    return ds
end

"""
    controller_loop!(controller, work; interval=300.0, stop=() -> false, max_rounds=nothing,
                     io=nothing, last=nothing)

[`manage!`](@ref) every `interval` seconds until `stop()` is true, `max_rounds` have run, nothing
is left (no runnable work on any partition and no job of ours pending or running), or the budget
is spent with nothing live. A `dry_run` policy runs one round: nothing it decides changes what the
next round would see. Returns the number of rounds.

With `io`, each round's decisions are printed as it happens; `last` (a `Ref`) receives the last
round's decisions. A round that throws is logged (`controller_round_failed`) and asked again —
except an `ArgumentError` (a wrong configuration), a dry run, or the fifth failure in a row,
which are raised.
"""
function controller_loop!(
    ctl::JobController,
    work;
    interval::Real=300.0,
    stop=() -> false,
    max_rounds::Union{Integer,Nothing}=nothing,
    io::Union{IO,Nothing}=nothing,
    last::Union{Base.RefValue,Nothing}=nothing,
)
    rounds = 0
    while !stop()
        rounds += 1
        decisions = try
            manage!(ctl, work)
        catch e
            e isa InterruptException && rethrow()
            # One round that failed (the work could not be counted, the ledger not written) is
            # not the end of the controller: said, nothing submitted, and asked again.
            log_event(
                ctl.log,
                :controller_round_failed;
                level=:error,
                round=rounds,
                err=_short_err(e),
            )
            io === nothing || println(io, "round $rounds failed: ", _short_err(e))
            # A wrong configuration is not a round to try again: it fails the same way for ever
            # (a `max_key_time` profile with no cost model was "round N failed", every round).
            e isa ArgumentError && rethrow()
            (max_rounds !== nothing && rounds >= max_rounds) && break
            ctl.policy.dry_run && rethrow()
            sleep(interval)
            continue
        end
        # With `io`, each round is said as it happens: a loop that printed nothing until it
        # ended looked the same whether every round was refused or none.
        if io !== nothing
            println(io, "round $rounds:")
            for d in decisions
                println(io, "  ", rpad(d.action, 7), rpad(d.partition, 12), d.reason)
            end
            flush(io)
        end
        last === nothing || (last[] = decisions)
        live = _ledger_live(ctl.ledger)
        idle = all(
            d -> d.action === :hold && startswith(d.reason, "nothing runnable"), decisions
        )
        (idle && !live) && break
        # The budget is spent and nothing of ours is live: no later round can submit either.
        # (It polled for ever, each round a refusal.)
        spent =
            !isempty(decisions) &&
            all(d -> d.action === :refuse && startswith(d.reason, "budget"), decisions)
        (spent && !live) && break
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
export campaign_work, JobController, controller_loop!, forget_job!
