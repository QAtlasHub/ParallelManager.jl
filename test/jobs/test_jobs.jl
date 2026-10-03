# Jobs (#66): submissions decided from what is left, behind a scheduler interface, inside a budget.

using SweepRunner, Test, DataVault, ParamIO, JSON3, Distributed
using SweepRunner: submit, cancel, job_states, remaining_time, shrink

const _JB_CFG = joinpath(@__DIR__, "..", "run", "fixtures", "study.toml")

# A partition: 2 nodes x 4 slots for 30 min = 14400 worker-seconds, 1 node-hour per job.
function _jb_part(; kw...)
    return PartitionPolicy(;
        name="short",
        nodes=2,
        time_limit=1800.0,
        script="/x/run.sh",
        slots_per_node=4,
        kw...,
    )
end

function _jb_policy(parts=[_jb_part(; max_jobs=10)]; kw...)
    return JobPolicy(; name="t", partitions=parts, budget_node_hours=100.0, kw...)
end

_jb_work(units, cost=NaN) = profile -> (; units=units, cost=Float64(cost), longest=0.0)

_jb_ledger() = Ledger(joinpath(mktempdir(), "ledger.json"))

function _jb_events(outdir)
    logs = filter(f -> startswith(f, "events_jobs_"), readdir(outdir))
    return [JSON3.read(l) for f in logs for l in readlines(joinpath(outdir, f))]
end

@testset "slurm time strings" begin
    s = SweepRunner._slurm_seconds
    @test s("30:00") == 1800.0
    @test s("1:00:00") == 3600.0
    @test s("1-02:03:04") == 86400 + 2 * 3600 + 3 * 60 + 4
    @test s("0:07") == 7.0
    @test s("UNLIMITED") == Inf
    @test s("N/A") == 0.0
    @test s("") == 0.0
    @test SweepRunner._slurm_minutes(1800) == "30"
    @test SweepRunner._slurm_minutes(1801) == "31"          # rounded up, never short
    @test SweepRunner._slurm_minutes(5) == "1"
end

@testset "SlurmScheduler: the commands it runs and what it reads back" begin
    seen = Vector{String}[]
    answers = Dict{String,Any}(
        "sbatch" => "4242;cluster\n",
        "squeue" =>
            "\"4242|t-short|short|RUNNING|2|30:00|10:00\"\n" *
            "4243|t-short|short|PENDING|2|30:00|0:00\n" *
            "4244|other|long|COMPLETING|16|1-00:00:00|23:59:59\n",
        "scancel" => "",
        "scontrol" => "",
    )
    run = cmd -> (push!(seen, collect(cmd.exec)); answers[cmd.exec[1]])
    s = SlurmScheduler(; user="me", run=run)

    spec = JobSpec(;
        name="t-short",
        partition="short",
        nodes=2,
        time_limit=1800.0,
        script="/x/run.sh",
        env=Dict("B" => "2", "A" => "1"),
        args=["--flag"],
    )
    @test submit(s, spec) == "4242"
    @test seen[end] == [
        "sbatch",
        "--parsable",
        "-J",
        "t-short",
        "-p",
        "short",
        "-N",
        "2",
        "-t",
        "30",
        "--export=ALL,A=1,B=2",
        "/x/run.sh",
        "--flag",
    ]

    js = job_states(s)
    @test seen[end][1:4] == ["squeue", "-h", "-u", "me"]
    @test [j.id for j in js] == ["4242", "4243", "4244"]
    @test [j.state for j in js] == [:running, :pending, :other]
    @test (js[1].nodes, js[1].time_limit, js[1].elapsed) == (2, 1800.0, 600.0)
    @test js[3].time_limit == 86400.0

    answers["squeue"] = "20:00\n"
    @test remaining_time(s, "4242") == 1200.0
    @test seen[end] == ["squeue", "-h", "-j", "4242", "-o", "%L"]
    answers["squeue"] = "\n"
    @test remaining_time(s, "4242") === nothing

    @test cancel(s, "4242")
    @test seen[end] == ["scancel", "4242"]
    @test shrink(s, "4242", 1)
    @test seen[end] == ["scontrol", "update", "JobId=4242", "NumNodes=1"]

    # A scheduler that does not answer is an error, never "no jobs".
    answers["squeue"] = nothing
    @test_throws ErrorException job_states(s)
    answers["sbatch"] = nothing
    @test_throws ErrorException submit(s, spec)
    answers["scancel"] = nothing
    @test cancel(s, "1") == false
end

@testset "Ledger: node-hours used and committed, kept across restarts" begin
    l = _jb_ledger()
    spec = JobSpec(; name="t-a", partition="a", nodes=4, time_limit=7200.0, script="s")
    SweepRunner.record_submit!(l, "1", spec)
    SweepRunner.record_submit!(l, "2", spec)
    nh = node_hours(l)
    @test nh.used == 0.0
    @test nh.committed == 2 * 4 * 2.0                        # two jobs, 4 nodes, 2 h each

    # Job 1 has run for an hour; job 2 is gone from the queue without ever being seen running.
    SweepRunner.observe!(l, [JobState("1", "t-a", "a", :running, 4, 7200.0, 3600.0)])
    nh = node_hours(l)
    @test nh.used == 4.0
    @test nh.committed == 4.0
    @test l.jobs["2"]["ended"] == true
    @test nh.by_partition["a"] == (; used=4.0, committed=4.0)

    SweepRunner.observe!(l, JobState[])                      # job 1 ended too
    nh = node_hours(l)
    @test (nh.used, nh.committed) == (4.0, 0.0)

    SweepRunner.save_ledger(l)
    again = Ledger(l.path)
    @test node_hours(again).used == 4.0
    @test again.jobs["1"]["partition"] == "a"
end

@testset "decide: nothing runnable means nothing submitted" begin
    ds = decide(_jb_policy(), _jb_work(0), JobState[], _jb_ledger())
    @test only(ds).action === :hold
    @test occursin("nothing runnable", only(ds).reason)
end

@testset "decide: jobs sized to what is left, never more slots than units" begin
    # 100 units x 600 s = 60000 worker-seconds; a job holds 8 slots x 1800 s = 14400.
    ds = decide(_jb_policy(), _jb_work(100, 60000), JobState[], _jb_ledger())
    @test length(ds) == 5                                    # ceil(60000 / 14400)
    @test all(d -> d.action === :submit, ds)
    @test all(d -> d.node_hours == 1.0, ds)
    spec = ds[1].spec
    @test (spec.name, spec.partition, spec.nodes, spec.time_limit) ==
        ("t-short", "short", 2, 1800.0)
    # 10 units that are long: the cost asks for 5 jobs, but 10 units fill at most 2 jobs' slots.
    @test length(decide(_jb_policy(), _jb_work(10, 60000), JobState[], _jb_ledger())) == 2
    # No cost model: units x key_time (the partition's, else the policy's default).
    @test length(decide(_jb_policy(), _jb_work(100), JobState[], _jb_ledger())) == 5
    quick = [_jb_part(; max_jobs=10, key_time=60.0)]
    @test length(decide(_jb_policy(quick), _jb_work(100), JobState[], _jb_ledger())) == 1
end

@testset "decide: what is already there counts" begin
    running(id; elapsed=0.0) =
        JobState(id, "t-short", "short", :running, 2, 1800.0, elapsed)
    # One fresh job covers 14400 worker-seconds.
    ds = decide(_jb_policy(), _jb_work(20, 12000), [running("1")], _jb_ledger())
    @test only(ds).action === :hold
    @test occursin("already cover", only(ds).reason)
    # Near its end it does not, and one more is asked for.
    ds = decide(
        _jb_policy(), _jb_work(20, 12000), [running("1"; elapsed=1700.0)], _jb_ledger()
    )
    @test [d.action for d in ds] == [:submit]
    # As many slots as units are already there: another job would have nothing to take.
    ds = decide(_jb_policy(), _jb_work(8, 1e9), [running("1")], _jb_ledger())
    @test only(ds).action === :hold
    # Jobs that are not ours do not count.
    theirs = JobState("9", "someone", "short", :running, 2, 1800.0, 0.0)
    ds = decide(_jb_policy(), _jb_work(20, 12000), [theirs], _jb_ledger())
    @test [d.action for d in ds] == [:submit]
end

@testset "decide: max_jobs, per partition and overall" begin
    ds = decide(
        _jb_policy([_jb_part(; max_jobs=2)]), _jb_work(100, 60000), JobState[], _jb_ledger()
    )
    @test count(d -> d.action === :submit, ds) == 2
    two = [_jb_part(; max_jobs=10), _jb_part(; name="long", max_jobs=10)]
    ds = decide(_jb_policy(two; max_jobs=3), _jb_work(100, 60000), JobState[], _jb_ledger())
    @test count(d -> d.action === :submit, ds) == 3
    @test ds[end].action === :hold && occursin("max_jobs", ds[end].reason)
    live = [JobState("1", "t-short", "short", :pending, 2, 1800.0, 0.0)]
    ds = decide(
        _jb_policy([_jb_part(; max_jobs=1)]), _jb_work(100, 60000), live, _jb_ledger()
    )
    @test only(ds).action === :hold && occursin("max_jobs", only(ds).reason)
end

@testset "decide: the budget is a refusal" begin
    # 1 node-hour per job, a budget of 2.5: two are submitted, the third refused.
    p = _jb_policy(; budget_node_hours=2.5)
    ds = decide(p, _jb_work(100, 60000), JobState[], _jb_ledger())
    @test [d.action for d in ds] == [:submit, :submit, :refuse]
    @test occursin("budget", ds[end].reason)
    # What earlier jobs used counts against it.
    l = _jb_ledger()
    spec = JobSpec(;
        name="t-short", partition="short", nodes=2, time_limit=3600.0, script="s"
    )
    SweepRunner.record_submit!(l, "1", spec)
    SweepRunner.observe!(
        l, [JobState("1", "t-short", "short", :running, 2, 3600.0, 3600.0)]
    )
    SweepRunner.observe!(l, JobState[])                       # ended after 2 node-hours
    ds = decide(p, _jb_work(100, 60000), JobState[], l)
    @test [d.action for d in ds] == [:refuse]
end

@testset "decide: each partition asks about the work its own profile can take" begin
    asked = Any[]
    work =
        profile -> (
            push!(asked, profile);
            (; units=profile == "short" ? 0 : 8, cost=NaN, longest=0.0)
        )
    parts = [_jb_part(; profile="short"), _jb_part(; name="long", profile="large")]
    ds = decide(_jb_policy(parts), work, JobState[], _jb_ledger())
    @test asked == ["short", "large"]
    @test [(d.partition, d.action) for d in ds] == [("short", :hold), ("long", :submit)]
    @test ds[2].spec.env["SWEEPRUNNER_PROFILE"] == "large"
end

@testset "manage!: a dry run decides and logs but submits nothing" begin
    outdir = mktempdir()
    sched = MockScheduler()
    ctl = JobController(sched, _jb_policy(), outdir)
    ds = manage!(ctl, _jb_work(100, 60000))
    @test count(d -> d.action === :submit, ds) == 5
    @test isempty(sched.submitted)
    @test isempty(ctl.ledger.jobs)
    ev = _jb_events(outdir)
    @test length(ev) == 5
    @test all(e -> e.kind == "job_decision" && e.dry_run == true, ev)
    @test occursin("dry run", sprint(io -> print_decisions(io, ds, ctl.ledger, ctl.policy)))
end

@testset "manage!: submissions are recorded, and the next round sees them" begin
    outdir = mktempdir()
    sched = MockScheduler()
    ctl = JobController(sched, _jb_policy(; dry_run=false), outdir)
    ds = manage!(ctl, _jb_work(100, 60000))
    @test length(sched.submitted) == 5
    @test length(ctl.ledger.jobs) == 5
    @test node_hours(ctl.ledger).committed == 5.0
    @test count(e -> e.kind == "job_submitted", _jb_events(outdir)) == 5
    @test isfile(ctl.ledger.path)

    # The same work, with those five pending: nothing more.
    ds = manage!(ctl, _jb_work(100, 60000))
    @test only(ds).action === :hold
    @test length(sched.submitted) == 5

    # A new controller on the same outdir starts from the ledger on disk.
    ctl2 = JobController(sched, _jb_policy(; dry_run=false, budget_node_hours=5.5), outdir)
    empty!(sched.jobs)                                         # they all ended, unused
    ds = manage!(ctl2, _jb_work(100, 60000))
    @test count(j -> j["ended"] == true, values(ctl2.ledger.jobs)) == 5    # the first five
    @test count(d -> d.action === :submit, ds) == 5            # nothing was used: 5.5 allows 5
end

@testset "controller_loop!: stops when nothing is left and nothing of ours is live" begin
    outdir = mktempdir()
    sched = MockScheduler()
    ctl = JobController(sched, _jb_policy(; dry_run=false), outdir)
    left = Ref(8)
    work = profile -> begin
        w = (; units=left[], cost=NaN, longest=0.0)
        # Each round, whatever was submitted has run and finished the work.
        if !isempty(sched.jobs)
            empty!(sched.jobs)
            left[] = 0
        end
        return w
    end
    rounds = controller_loop!(ctl, work; interval=0.01, max_rounds=10)
    @test rounds == 3            # submit; held while it runs; nothing left and none live
    @test length(sched.submitted) == 1
end

@testset "load_job_policy and `sweeprunner jobs`: from the campaign's own file" begin
    dir = mktempdir()
    out = joinpath(dir, "out")
    cp(_JB_CFG, joinpath(dir, "study.toml"))
    meta = joinpath(dir, "campaign.toml")
    body = dry -> """
    [campaign]
    name   = "j"
    outdir = "$out"

    [[study]]
    name   = "pm"
    stages = { phase1 = "study.toml" }

    [profile.short]
    skip_stages = []

    [jobs]
    name              = "j"
    budget_node_hours = 10
    max_jobs          = 4
    dry_run           = $dry
    default_key_time  = "30min"

    [[jobs.partition]]
    name           = "i8cpu"
    nodes          = 1
    time_limit     = "30min"
    script         = "batch/run.sh"
    profile        = "short"
    max_jobs       = 2
    slots_per_node = 2
    env            = { MODE = "x" }
    """
    write(meta, body(true))
    p = load_job_policy(meta)
    @test (p.name, p.budget_node_hours, p.max_jobs, p.dry_run) == ("j", 10.0, 4, true)
    @test p.default_key_time == 1800.0
    part = only(p.partitions)
    @test (part.name, part.nodes, part.time_limit, part.profile) ==
        ("i8cpu", 1, 1800.0, "short")
    @test part.script == joinpath(dir, "batch", "run.sh")
    @test part.env == Dict("MODE" => "x")
    @test_throws ArgumentError load_job_policy(joinpath(dir, "study.toml"))

    c = load_campaign(meta)
    work = campaign_work(s -> (;), c)
    nkeys = length(ParamIO.expand(ParamIO.load(_JB_CFG)))
    @test work("short").units == nkeys
    @test isnan(work("short").cost)

    sched = MockScheduler()
    saved = SweepRunner._CLI_SCHEDULER[]
    SweepRunner._CLI_SCHEDULER[] = () -> sched
    try
        # dry_run = true in the file: --submit alone does not submit.
        text = sprint(io -> (@test SweepRunner.cli(["jobs", meta, "--submit"]; io=io) == 0))
        @test occursin("dry run", text) && occursin("submit", text)
        @test isempty(sched.submitted)
        # dry_run = false in the file: without --submit it still does not.
        write(meta, body(false))
        @test SweepRunner.cli(["jobs", meta]; io=IOBuffer()) == 0
        @test isempty(sched.submitted)
        # Both: nkeys units of 30 min on 2 slots for 30 min -> 2 jobs (max_jobs = 2).
        text = sprint(io -> (@test SweepRunner.cli(["jobs", meta, "--submit"]; io=io) == 0))
        @test length(sched.submitted) == 2
        @test sched.submitted[1].env ==
            Dict("MODE" => "x", "SWEEPRUNNER_PROFILE" => "short")
        @test !occursin("dry run", text)

        # Once the work is done there is nothing to submit, whatever the queue looks like.
        empty!(sched.jobs)
        v = DataVault.Vault(joinpath(dir, "study.toml"); run="phase1", outdir=out)
        run!(k -> Dict{String,Any}("x" => 1), v, DataVault.keys(v))
        text = sprint(io -> SweepRunner.cli(["jobs", meta, "--submit"]; io=io))
        @test occursin("nothing runnable", text)
        @test length(sched.submitted) == 2
    finally
        SweepRunner._CLI_SCHEDULER[] = saved
        rm(dir; recursive=true, force=true)
    end
end

@testset "a master that is holding nodes for a few long units leaves on purpose" begin
    nprocs() > 1 && rmprocs(workers())
    addprocs(3; exeflags="--project=$(dirname(Base.active_project()))")
    outdir = mktempdir()
    try
        @everywhere workers() Core.eval(
            Main, :(using SweepRunner, DataVault, ParamIO, Distributed)
        )
        v = DataVault.Vault(_JB_CFG; run="idle", outdir=outdir)
        k = DataVault.keys(v)[1]
        # One long unit on three workers: the queue is empty and a third of the pool is busy.
        work = key -> begin
            for step in 1:600
                SweepRunner.report_progress(step; of=600)
                SweepRunner.stop_point(; poll=0)
                sleep(0.1)
            end
            return Dict{String,Any}("x" => 1)
        end
        opts = RunOpts(; min_busy_fraction=0.5, idle_grace=1.0, control_interval=0.2)
        t0 = time()
        r = run!(work, v, [k]; opts=opts)
        @test time() - t0 < 45                                  # not the 60 s the unit would take
        @test r.stopped_by === :underused
        @test (r.done, r.stop, r.err) == (0, 1, 0)
        @test !DataVault.is_running(v, k)
        @test read_progress(v)[ParamIO.canonical(k)].step >= 1   # where the next job resumes
        logs = filter(f -> startswith(f, "events_"), readdir(outdir))
        ev = [JSON3.read(l) for f in logs for l in readlines(joinpath(outdir, f))]
        u = only([e for e in ev if e.kind == "underused"])
        @test (u.busy, u.workers) == (1, 3)

        # With the threshold off (the default) the same unit runs to its end.
        v2 = DataVault.Vault(_JB_CFG; run="busy", outdir=outdir)
        quick = key -> (sleep(2.5); Dict{String,Any}("x" => 1))
        r = run!(quick, v2, [DataVault.keys(v2)[1]]; opts=RunOpts(; control_interval=0.2))
        @test r.done == 1 && r.stopped_by === nothing
    finally
        rmprocs(workers())
        note_workers!(; planned=0, launched=0)
        rm(outdir; recursive=true, force=true)
    end
end
