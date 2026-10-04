# CLI — asking a sweep from a shell.
#
#     sweeprunner status <outdir> [--workers] [--json]
#     sweeprunner locks  <outdir> [--reap]
#     sweeprunner account <outdir>
#     sweeprunner costs <outdir>
#     sweeprunner campaign <meta.toml> [--profile NAME] [--studies a,b]
#     sweeprunner jobs <meta.toml> [--submit] [--loop SECONDS] [--forget JOBID]
#     sweeprunner pause|resume|stop|cancel|prioritise|resize|drain|enqueue <outdir> [options]
#
# `bin/sweeprunner` is the wrapper; `julia -e 'using SweepRunner; SweepRunner.cli(ARGS)' -- …` is
# the same thing. Everything here reads (or, for requests, writes one small file under) the
# sweep's state directory, so it runs on a login node while the job runs on the compute nodes.

const _CLI_USAGE = """
usage: sweeprunner <command> <outdir> [options]

  status <outdir> [--workers] [--json]
      What every master under <outdir> is doing: task counts, workers planned / launched /
      joined / busy, cores in use, nodes with no worker, warnings. --workers adds one line per
      worker; --json prints the status files as one JSON array.

  locks <outdir> [--reap]
      Every .running lock under <outdir>: who holds it, heartbeat and progress age, and whether
      its holder's master says it is held, dead, or cannot be asked. Removes nothing, unless
      --reap is given: then the locks whose holder is shown to be gone (dead) are removed, on
      every key under <outdir>, and nothing else is.

  account <outdir>
      Where each master's core-hours went: computing (kept / lost), start-up, workers that never
      started, idle by reason.

  costs <outdir>
      What finished keys cost, per class: count, median and p90 wall time, cores and how much
      of them was used, peak memory.

  campaign <meta.toml> [--profile NAME] [--studies a,b]
      Validate a meta config and print the stages a job would run, in order. Exit code 1 when
      it is not launchable.

  jobs <meta.toml> [--submit] [--loop SECONDS] [--forget JOBID]
      Decide what to submit for the campaign in <meta.toml> from its [jobs] table, what is left
      and what the scheduler lists, and print each decision with its reason and the node-hour
      account. Nothing is submitted without --submit AND `dry_run = false` in the file.
      --loop repeats every SECONDS until nothing is left. --forget marks one job of the ledger
      as ended, by hand: for a job whose end the controller cannot see.

  pause | resume <outdir>
  stop <outdir> [--select name=v1,v2 ...] [--node HOST] [--grace SECONDS]
  cancel <outdir> --select name=v1,v2 [...] [--samples 1,2] [--running] [--grace SECONDS]
  prioritise <outdir> --select name=v1,v2 [...] [--samples 1,2]
  resize <outdir> --n N
  drain <outdir> --node HOST
  enqueue <outdir> --config FILE
      Requests to the masters running under <outdir>; see `SweepRunner.control!`. Limit them with
      --project NAME, --run NAME, --master ID. Prints the request ids. --wait SECONDS waits for a
      master to acknowledge and prints what it did.
      Exit codes: 0 sent (and, with --wait, applied); 1 no sweep state there; 3 no master is
      running, so nothing will apply it; 4 nobody acknowledged in time; 5 a master could not
      apply it.
"""

"""
    cli(args=ARGS; io=stdout) -> Int

The `sweeprunner` command line. Returns the exit code:

| code | meaning |
|---|---|
| `0` | done |
| `1` | nothing to act on (no sweep state under `<outdir>`; a campaign that is not launchable), or `locks --reap` could not remove a dead lock |
| `2` | usage error (the usage text is printed to `io`) |
| `3` | a request was written and no master that reports a status is running to apply it |
| `4` | `--wait`: a listening master did not acknowledge in time (it is named) |
| `5` | `--wait`: a master answered that the request was refused or is unsupported |
| `6` | `jobs`: the round refused — the scheduler could not be asked or trusted, the ledger could not be used, or the budget was reached |

```
sweeprunner status out/campaign --workers
```
"""
function cli(args::AbstractVector{<:AbstractString}=ARGS; io::IO=stdout)
    if isempty(args) || args[1] in ("-h", "--help", "help")
        print(io, _CLI_USAGE)
        return isempty(args) ? 2 : 0
    end
    cmd = args[1]
    rest = args[2:end]
    Symbol(cmd) in _CONTROL_OPS && return _cli_control(io, Symbol(cmd), rest)
    flags = Set(a for a in rest if startswith(a, "--"))
    pos = [a for a in rest if !startswith(a, "--")]
    if cmd == "status"
        length(pos) == 1 || return _cli_usage(io, "status takes one <outdir>")
        if "--json" in flags
            JSON3.write(io, read_status(pos[1]))
            println(io)
        else
            print_status(io, pos[1]; workers="--workers" in flags)
        end
        return 0
    end
    if cmd == "campaign"
        return _cli_campaign(io, rest)
    end
    if cmd == "jobs"
        return _cli_jobs(io, rest)
    end
    if cmd == "account"
        length(pos) == 1 || return _cli_usage(io, "account takes one <outdir>")
        print_account(io, pos[1])
        return 0
    end
    if cmd == "costs"
        length(pos) == 1 || return _cli_usage(io, "costs takes one <outdir>")
        print_costs(io, pos[1])
        return 0
    end
    if cmd == "locks"
        length(pos) == 1 || return _cli_usage(io, "locks takes one <outdir>")
        print_locks(io, pos[1])
        if "--reap" in flags
            r = reap_dead_locks!(pos[1])
            println(io, "reaped $(r.reaped) of $(r.dead) dead lock(s)")
            r.changed_hands == 0 ||
                println(io, "$(r.changed_hands) had been released or taken in the meantime")
            if r.failed > 0
                println(io, "$(r.failed) could not be removed:")
                foreach(f -> println(io, "  ", f), r.failures)
                return 1
            end
        end
        return 0
    end
    return _cli_usage(io, "unknown command: $cmd")
end

function _cli_campaign(io::IO, rest)
    meta = nothing
    profile = nothing
    studies = nothing
    i = 1
    while i <= length(rest)
        a = rest[i]
        if a == "--profile" || a == "--studies"
            i < length(rest) || return _cli_usage(io, "$a needs a value")
            v = rest[i += 1]
            a == "--profile" ? (profile = v) : (studies = String.(split(v, ',')))
        elseif startswith(a, "--")
            return _cli_usage(io, "unknown option: $a")
        elseif meta === nothing
            meta = a
        else
            return _cli_usage(io, "campaign takes one <meta.toml>")
        end
        i += 1
    end
    meta === nothing && return _cli_usage(io, "campaign needs a <meta.toml>")
    isfile(meta) || return _cli_usage(io, "no such file: $meta")
    c = load_campaign(meta)
    show(io, c)
    report = validate_campaign(c)
    show(io, report)
    launchable(report) || return 1
    plan = try
        plan_campaign(c; studies=studies, profile=profile)
    catch e
        e isa ArgumentError || rethrow()
        return _cli_usage(io, e.msg)
    end
    println(io, "plan", profile === nothing ? "" : " (profile $profile)", ":")
    for (n, s) in enumerate(plan)
        println(io, "  ", n, ". ", stage_id(s))
    end
    return 0
end

# The scheduler `sweeprunner jobs` talks to. A test replaces it.
const _CLI_SCHEDULER = Ref{Any}(() -> SlurmScheduler())

function _cli_jobs(io::IO, rest)
    meta = nothing
    go = false
    every = nothing
    forget = nothing
    i = 1
    while i <= length(rest)
        a = rest[i]
        if a == "--submit"
            go = true
        elseif a == "--forget"
            i < length(rest) || return _cli_usage(io, "--forget needs a job id")
            forget = rest[i += 1]
        elseif a == "--loop"
            i < length(rest) || return _cli_usage(io, "--loop needs a value")
            every = tryparse(Float64, rest[i += 1])
            every === nothing && return _cli_usage(io, "--loop takes seconds")
        elseif startswith(a, "--")
            return _cli_usage(io, "unknown option: $a")
        elseif meta === nothing
            meta = a
        else
            return _cli_usage(io, "jobs takes one <meta.toml>")
        end
        i += 1
    end
    meta === nothing && return _cli_usage(io, "jobs needs a <meta.toml>")
    isfile(meta) || return _cli_usage(io, "no such file: $meta")
    c = load_campaign(meta)
    report = validate_campaign(c)
    if !launchable(report)
        show(io, report)
        return 1
    end
    policy = try
        load_job_policy(meta)
    catch e
        e isa ArgumentError || rethrow()
        return _cli_usage(io, e.msg)
    end
    # Submitting takes both: the flag here and `dry_run = false` in the file.
    if policy.dry_run || !go
        policy = JobPolicy(;
            name=policy.name,
            partitions=policy.partitions,
            budget_node_hours=policy.budget_node_hours,
            max_jobs=policy.max_jobs,
            dry_run=true,
            default_key_time=policy.default_key_time,
        )
    end
    ctl = JobController(_CLI_SCHEDULER[](), policy, c.outdir)
    if forget !== nothing
        if forget_job!(ctl, forget)
            println(io, "job $forget is marked ended in the ledger")
            return 0
        end
        println(io, "the ledger has no live job $forget")
        return 1
    end
    work = campaign_work(s -> (;), c)
    try
        if every === nothing
            ds = manage!(ctl, work)
            print_decisions(io, ds, ctl.ledger, policy)
            # A round in which nothing could be decided, or a submission was refused, is not
            # a success for whoever calls this from a script.
            any(d -> d.action === :refuse, ds) && return 6
        else
            controller_loop!(ctl, work; interval=every, io=io)
            print_decisions(io, Decision[], ctl.ledger, policy)
        end
    catch e
        # Only the one error this message is about: any other `ArgumentError` used to be
        # turned into "no cost model" too.
        (e isa ArgumentError && occursin("max_key_time", e.msg)) || rethrow()
        # A profile with `max_key_time` needs a cost per key, and the command line has none.
        println(io, "sweeprunner jobs: ", e.msg)
        println(
            io,
            "  The command line has no cost model. Run the controller from Julia with ",
            "`campaign_work(open_stage, campaign; cost = …)`, or use a profile without ",
            "max_key_time for this partition.",
        )
        return 2
    end
    return 0
end

# Options that take a value, and the ones that do not.
const _CLI_VALUED = (
    "--select",
    "--samples",
    "--node",
    "--grace",
    "--n",
    "--config",
    "--project",
    "--run",
    "--master",
    "--wait",
)
const _CLI_SWITCHES = ("--running", "--interrupt")

# `32` -> 32, `0.5` -> 0.5, `true` -> true, anything else stays a string.
function _cli_value(s::AbstractString)
    for T in (Int, Float64, Bool)
        v = tryparse(T, s)
        v === nothing || return v
    end
    return String(s)
end

function _cli_control(io::IO, op::Symbol, rest)
    outdir = nothing
    select = Dict{String,Vector{Any}}()
    kw = Dict{Symbol,Any}()
    wait_s = nothing
    i = 1
    while i <= length(rest)
        a = rest[i]
        if a in _CLI_SWITCHES
            kw[Symbol(a[3:end])] = true
        elseif a in _CLI_VALUED
            i < length(rest) || return _cli_usage(io, "$a needs a value")
            v = rest[i += 1]
            if a == "--select"
                nv = split(v, '='; limit=2)
                length(nv) == 2 || return _cli_usage(io, "--select takes name=v1,v2")
                select[String(nv[1])] = Any[_cli_value(x) for x in split(nv[2], ',')]
            elseif a == "--samples"
                kw[:samples] = [parse(Int, x) for x in split(v, ',')]
            elseif a == "--grace"
                kw[:grace] = parse(Float64, v)
            elseif a == "--wait"
                wait_s = tryparse(Float64, v)
                wait_s === nothing && return _cli_usage(io, "--wait takes seconds")
            elseif a == "--n"
                kw[:n] = parse(Int, v)
            else
                kw[Symbol(a[3:end])] = String(v)
            end
        elseif startswith(a, "--")
            return _cli_usage(io, "unknown option: $a")
        elseif outdir === nothing
            outdir = a
        else
            return _cli_usage(io, "$op takes one <outdir>")
        end
        i += 1
    end
    outdir === nothing && return _cli_usage(io, "$op needs an <outdir>")
    isempty(select) || (kw[:select] = select)
    ids = try
        control!(outdir, op; kw...)
    catch e
        e isa ArgumentError || rethrow()
        return _cli_usage(io, e.msg)
    end
    if isempty(ids)
        println(io, "no sweep state under $outdir: nothing to send the request to")
        return 1
    end
    foreach(id -> println(io, id), ids)
    # A master applies only requests made after it started. With none running, this one is
    # applied by nobody — not by the next job either — and that is not success.
    listening = masters_listening(
        outdir; project=get(kw, :project, nothing), run=get(kw, :run, nothing)
    )
    if isempty(listening)
        println(
            io,
            "no master is seen running under $outdir: the request was written, and nothing ",
            "will apply it (a master run with status_interval = 0 writes no status and is ",
            "not seen here)",
        )
        return 3
    end
    if wait_s === nothing
        # Whether it will be applied, refused or unsupported is not known without waiting.
        println(
            io,
            "sent to ",
            length(listening),
            " master(s); not waited for (--wait <secs> says whether each applied it)",
        )
        return 0
    end
    acks = _acks_under(outdir, ids, wait_s; masters=listening)
    if isempty(acks)
        println(
            io, "no master acknowledged within $(wait_s) s (", join(listening, ", "), ")"
        )
        return 4
    end
    code = 0
    for a in acks
        d = a["detail"]
        println(io, a["master"], ": ", isempty(d) ? "applied" : JSON3.write(d))
        (haskey(d, "error") || haskey(d, "unsupported")) && (code = 5)
    end
    # Every master that is listening, not the first to answer: the ones that did not are named.
    missing = setdiff(listening, [String(a["master"]) for a in acks])
    if !isempty(missing)
        println(io, "no acknowledgement within $(wait_s) s from: ", join(missing, ", "))
        code == 0 && (code = 4)
    end
    return code
end

# The acknowledgements of the requests `ids`, wherever under `outdir` they were sent, waiting up
# to `timeout` seconds for the first.
function _acks_under(outdir::AbstractString, ids, timeout::Real; masters=String[])
    t0 = time()
    while true
        acks = Dict{String,Any}[]
        base = joinpath(outdir, "sweeprunner")
        for p in readdir(base; join=true),
            r in (isdir(p) ? readdir(p; join=true) : String[])

            for id in ids
                dir = joinpath(r, "control", "acks", id)
                isdir(dir) || continue
                for f in readdir(dir; join=true)
                    a = _read_json(f)
                    a === nothing || push!(acks, a)
                end
            end
        end
        # With the masters known, wait for each of them; without, for the first answer.
        got = Set(String(get(a, "master", "")) for a in acks)
        done = isempty(masters) ? !isempty(acks) : all(m -> m in got, masters)
        (done || time() - t0 >= timeout) && return acks
        sleep(0.2)
    end
end

function _cli_usage(io::IO, msg::AbstractString)
    println(io, "sweeprunner: ", msg)
    print(io, _CLI_USAGE)
    return 2
end
