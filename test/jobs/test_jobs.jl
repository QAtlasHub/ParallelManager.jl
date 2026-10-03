# Jobs (#66): submissions decided from what is left, behind a scheduler interface, inside a budget.

using SweepRunner, Test, DataVault, ParamIO, JSON3, Distributed
using SweepRunner: Ledger, node_hours, decide, manage!, print_decisions
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
    t = SweepRunner._slurm_time
    @test t("1-02:03:04") == s("1-02:03:04")
    @test t("soon") === nothing                       # unreadable is not zero
    @test t("1-xx:00") === nothing
    @test t("x-01:00") === nothing
    @test SweepRunner._slurm_minutes(1800) == "30"
    @test SweepRunner._slurm_minutes(1801) == "31"          # rounded up, never short
    @test SweepRunner._slurm_minutes(5) == "1"
end

@testset "SlurmScheduler: the commands it runs and what it reads back" begin
    seen = Vector{String}[]
    answers = Dict{String,Any}(
        "sbatch" => "4242;cluster\n",
        "squeue" =>
            "\"4242|short|RUNNING|2|30:00|10:00|t-short\"\n" *
            "4243|short|PENDING|2|30:00|0:00|t-short\n" *
            "4244|long|COMPLETING|16|1-00:00:00|23:59:59|a|name|with|bars\n",
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
    @test js[3].name == "a|name|with|bars"            # the separator inside a job name
    # A line or a time that cannot be read refuses the whole answer: a job skipped is a job
    # that is not charged.
    answers["squeue"] = "4242|short|RUNNING|2|30:00\n"
    @test_throws ErrorException job_states(s)
    answers["squeue"] = "4242|short|RUNNING|2|soon|10:00|t-short\n"
    @test_throws ErrorException job_states(s)
    answers["squeue"] = "4242|short|RUNNING|two|30:00|10:00|t-short\n"
    @test_throws ErrorException job_states(s)

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
    SweepRunner.record_submit!(l, "1", spec; now=1000.0)
    SweepRunner.record_submit!(l, "2", spec; now=1000.0)
    nh = node_hours(l)
    @test nh.used == 0.0
    @test nh.committed == 2 * 4 * 2.0                        # two jobs, 4 nodes, 2 h each

    # Job 1 has run for an hour. Job 2 is not listed: absent once is not ended.
    seen = [JobState("1", "t-a", "a", :running, 4, 7200.0, 3600.0)]
    SweepRunner.observe!(l, seen; now=5000.0)
    @test l.jobs["2"]["ended"] == false
    @test l.jobs["2"]["missing"] == 1
    nh = node_hours(l)
    @test nh.used == 4.0
    @test nh.committed == 4.0 + 8.0                          # job 2 still commits its 2 h
    # Absent three polls in a row: ended. It was pending when last seen, 4200 s ago; it can
    # have run that long, and is billed for it — not for the 0 s that was last read.
    SweepRunner.observe!(l, seen; now=5100.0)
    SweepRunner.observe!(l, seen; now=5200.0)
    @test l.jobs["2"]["ended"] == true
    @test l.jobs["2"]["elapsed"] == 4200.0
    @test node_hours(l).by_partition["a"].used ≈ 4.0 + 4 * 4200 / 3600

    # A job in a state that is neither RUNNING nor PENDING is listed, so it exists.
    SweepRunner.observe!(
        l, [JobState("1", "t-a", "a", :other, 4, 7200.0, 3700.0)]; now=5300.0
    )
    @test l.jobs["1"]["ended"] == false
    @test l.jobs["1"]["state"] == "other"
    @test l.jobs["1"]["elapsed"] == 3700.0
    # A job taken as ended that is listed again is live again.
    SweepRunner.observe!(
        l, [JobState("2", "t-a", "a", :running, 4, 7200.0, 4300.0)]; now=5400.0
    )
    @test l.jobs["2"]["ended"] == false
    @test l.jobs["2"]["missing"] == 0
    # A vanished running job is billed up to its time limit at most.
    for t in (9.0e4, 9.1e4, 9.2e4)
        SweepRunner.observe!(l, JobState[]; now=t)
    end
    @test l.jobs["2"]["ended"] == true
    @test l.jobs["2"]["elapsed"] == 7200.0

    SweepRunner.save_ledger(l)
    again = Ledger(l.path)
    @test node_hours(again).used == node_hours(l).used
    @test again.jobs["1"]["partition"] == "a"
end

@testset "Ledger: a submission is on record before the scheduler answers" begin
    l = _jb_ledger()
    spec = JobSpec(; name="t-a", partition="a", nodes=2, time_limit=3600.0, script="s")
    tmp = SweepRunner.record_intent!(l, spec; now=100.0)
    @test isfile(l.path)                                     # saved at once
    @test Ledger(l.path).jobs[tmp]["state"] == "submitting"
    @test node_hours(l).committed == 2.0                     # it counts from here
    SweepRunner.confirm_submit!(l, tmp, "77")
    @test !haskey(l.jobs, tmp) && l.jobs["77"]["state"] == "pending"

    # sbatch timed out, so the id is not known — but the job is in the queue: found by name.
    tmp = SweepRunner.record_intent!(l, spec; now=200.0)
    listed = [
        JobState("77", "t-a", "a", :running, 2, 3600.0, 60.0),
        JobState("78", "t-a", "a", :pending, 2, 3600.0, 0.0),
        JobState("90", "other", "a", :pending, 2, 3600.0, 0.0),
    ]
    SweepRunner.observe!(l, listed; now=300.0)
    @test !haskey(l.jobs, tmp)
    @test l.jobs["78"]["state"] == "pending"
    @test !haskey(l.jobs, "90")                              # not ours: not adopted
    # One that never reached the queue stops counting after it has been absent long enough.
    tmp = SweepRunner.record_intent!(l, spec; now=400.0)
    for t in (500.0, 600.0)
        SweepRunner.observe!(l, listed; now=t)
        @test l.jobs[tmp]["ended"] == false
    end
    SweepRunner.observe!(l, listed; now=700.0)
    @test l.jobs[tmp]["ended"] == true
    @test l.jobs[tmp]["elapsed"] == 0.0                      # it never ran
end

@testset "policies refuse values that would switch the budget off" begin
    ok = (; name="p", nodes=1, time_limit=60.0, script="s")
    @test_throws ArgumentError PartitionPolicy(; ok..., nodes=0)
    @test_throws ArgumentError PartitionPolicy(; ok..., nodes=-2)
    @test_throws ArgumentError PartitionPolicy(; ok..., time_limit=NaN)
    @test_throws ArgumentError PartitionPolicy(; ok..., time_limit=-1.0)
    @test_throws ArgumentError PartitionPolicy(; ok..., time_limit=Inf)
    @test_throws ArgumentError PartitionPolicy(; ok..., slots_per_node=0)
    @test_throws ArgumentError PartitionPolicy(; ok..., max_jobs=-1)
    @test_throws ArgumentError PartitionPolicy(; ok..., key_time=0.0)
    p = PartitionPolicy(; ok...)
    @test_throws ArgumentError JobPolicy(; name="t", partitions=[p], budget_node_hours=NaN)
    @test_throws ArgumentError JobPolicy(; name="t", partitions=[p], budget_node_hours=-1)
    @test_throws ArgumentError JobPolicy(; name="t", partitions=[p], budget_node_hours=Inf)
    @test_throws ArgumentError JobPolicy(; name="", partitions=[p], budget_node_hours=1)
    @test_throws ArgumentError JobPolicy(; name="t", partitions=[p, p], budget_node_hours=1)
    @test_throws ArgumentError JobPolicy(;
        name="t", partitions=[p], budget_node_hours=1, default_key_time=0
    )
    @test JobPolicy(; name="t", partitions=[p], budget_node_hours=0).budget_node_hours ==
        0.0
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
    # Jobs that are not ours do not count — and "ours" is the exact name, not a prefix: a policy
    # named `t` does not claim the jobs of one named `t2`.
    for name in ("someone", "t2-short", "t-shorter", "t")
        theirs = JobState("9", name, "short", :running, 2, 1800.0, 0.0)
        ds = decide(_jb_policy(), _jb_work(20, 12000), [theirs], _jb_ledger())
        @test [d.action for d in ds] == [:submit]
    end
    # A job of ours in a state that is neither RUNNING nor PENDING still holds its place.
    odd = JobState("1", "t-short", "short", :other, 2, 1800.0, 0.0)
    ds = decide(_jb_policy(), _jb_work(20, 12000), [odd], _jb_ledger())
    @test only(ds).action === :hold
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
    @test length(ctl2.ledger.jobs) == 5
    empty!(sched.jobs)                                         # they all ended, unused
    # The scheduler lists nothing while the ledger has live jobs: not trusted at once. Nothing
    # is submitted on it, and it takes three such answers for the jobs to count as ended.
    for _ in 1:3
        ds = manage!(ctl2, _jb_work(100, 60000))
        @test all(d -> d.action === :refuse && occursin("not trusted", d.reason), ds)
    end
    @test length(sched.submitted) == 5
    @test all(j -> j["ended"] == true, values(ctl2.ledger.jobs))
    ds = manage!(ctl2, _jb_work(100, 60000))
    @test count(d -> d.action === :submit, ds) == 5            # next to nothing was used
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
    # submit; held while it runs; three rounds for its absence to count as ended; then nothing
    # left and none live.
    @test rounds == 6
    @test length(sched.submitted) == 1
end

@testset "controller_loop!: a round that fails is said, and the loop asks again (#113)" begin
    outdir = mktempdir()
    sched = MockScheduler()
    ctl = JobController(sched, _jb_policy(; dry_run=false), outdir)
    calls = Ref(0)
    work = profile -> begin
        calls[] += 1
        calls[] == 1 && error("the vault could not be read")
        return (; units=0, cost=NaN, longest=0.0)
    end
    rounds = controller_loop!(ctl, work; interval=0.01, max_rounds=10)
    @test rounds == 2                                          # failed, asked again, nothing left
    @test isempty(sched.submitted)
    failed = only([e for e in _jb_events(outdir) if e.kind == "controller_round_failed"])
    @test failed.round == 1
    @test occursin("could not be read", failed.err)
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

@testset "_run_command: stdout of a command that succeeds, nothing for one that does not" begin
    rc = SweepRunner._run_command
    @test rc(`echo 4242`) == "4242\n"
    @test rc(`false`) === nothing
    @test rc(`sleep 5`; timeout=0.3) === nothing                 # killed, not waited for
    @test rc(`definitely-not-a-command-xyz`) === nothing
    # The default scheduler runs real commands: with no sbatch on PATH a submit is an error,
    # not a silent nothing.
    s = SlurmScheduler(; user="nobody")
    if Sys.which("sbatch") === nothing
        spec = JobSpec(; name="x", partition="p", nodes=1, time_limit=60.0, script="/x")
        @test_throws ErrorException submit(s, spec)
        @test_throws ErrorException job_states(s)
        @test remaining_time(s, "1") === nothing
    end
    # A backend that does not say otherwise cannot shrink and knows no remaining time.
    m = MockScheduler()
    @test shrink(m, "1", 1) == false
    @test remaining_time(m, "1") === nothing
    id = submit(
        m, JobSpec(; name="x", partition="p", nodes=1, time_limit=60.0, script="/x")
    )
    @test remaining_time(m, id) == 60.0
    @test cancel(m, id) && !cancel(m, id)
end

@testset "manage!: a submission the scheduler refuses is logged and the round goes on" begin
    outdir = mktempdir()
    failing = SlurmScheduler(; user="me", run=cmd -> cmd.exec[1] == "squeue" ? "" : nothing)
    ctl = JobController(
        failing, _jb_policy([_jb_part(; max_jobs=2)]; dry_run=false), outdir
    )
    ds = manage!(ctl, _jb_work(100, 60000))
    @test count(d -> d.action === :submit, ds) == 2
    @test count(e -> e.kind == "job_submit_failed", _jb_events(outdir)) == 2
    # Whether the scheduler took them is not known, so they stay on record and committed...
    @test length(ctl.ledger.jobs) == 2
    @test all(j -> j["state"] == "submitting", values(ctl.ledger.jobs))
    @test node_hours(ctl.ledger).committed == 2.0
    @test node_hours(Ledger(ctl.ledger.path)).committed == 2.0     # and on disk
    # ...until they have been absent from the queue long enough.
    quiet = _jb_work(0)
    for _ in 1:3
        manage!(ctl, quiet)
    end
    @test node_hours(ctl.ledger).committed == 0.0
    @test node_hours(ctl.ledger).used == 0.0
end

@testset "cli jobs: usage errors, a file without [jobs], and --loop" begin
    io = IOBuffer()
    @test SweepRunner.cli(["jobs"]; io=io) == 2
    @test SweepRunner.cli(["jobs", "/no/such.toml"]; io=io) == 2
    @test SweepRunner.cli(["jobs", "a", "b"]; io=io) == 2
    @test SweepRunner.cli(["jobs", "a", "--bogus"]; io=io) == 2
    @test SweepRunner.cli(["jobs", "a", "--loop"]; io=io) == 2
    @test SweepRunner.cli(["jobs", "a", "--loop", "soon"]; io=io) == 2
    dir = mktempdir()
    try
        cp(_JB_CFG, joinpath(dir, "study.toml"))
        meta = joinpath(dir, "campaign.toml")
        head = "[campaign]\nname = \"l\"\noutdir = \"$(joinpath(dir, "out"))\"\n"
        study = "[[study]]\nname = \"pm\"\nstages = { phase1 = \"study.toml\" }\n"
        write(meta, head)                                           # not launchable: no study
        @test SweepRunner.cli(["jobs", meta]; io=io) == 1
        write(meta, head * study)                                   # no [jobs]
        @test SweepRunner.cli(["jobs", meta]; io=io) == 2
        write(meta, head * study * "[jobs]\nname = \"l\"\n")       # no budget
        @test SweepRunner.cli(["jobs", meta]; io=io) == 2
        jobs = "[jobs]\nname = \"l\"\nbudget_node_hours = 5\n"
        write(meta, head * study * jobs)                            # no partition
        @test SweepRunner.cli(["jobs", meta]; io=io) == 2
        part =
            "[[jobs.partition]]\nname = \"p\"\nnodes = 1\ntime_limit = \"1h\"\n" *
            "script = \"/abs/run.sh\"\nkey_time = \"1min\"\n"
        write(meta, head * study * jobs * part)
        pol = load_job_policy(meta)
        @test only(pol.partitions).script == "/abs/run.sh"
        @test only(pol.partitions).key_time == 60.0
        sched = MockScheduler()
        saved = SweepRunner._CLI_SCHEDULER[]
        SweepRunner._CLI_SCHEDULER[] = () -> sched
        try
            # A dry-run loop: it decides, submits nothing, and ends when a round changes nothing
            # it could wait for.
            text = sprint(
                io2 ->
                    (@test SweepRunner.cli(["jobs", meta, "--loop", "0.01"]; io=io2) == 0),
            )
            @test occursin("budget", text)
            @test isempty(sched.submitted)
        finally
            SweepRunner._CLI_SCHEDULER[] = saved
        end
    finally
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
        @test u.cores_busy * 3 == u.cores                       # counted in cores (#113)

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

@testset "manage!: a scheduler answer that cannot be read submits nothing (#104)" begin
    outdir = mktempdir()
    garbled = SlurmScheduler(; user="me", run=cmd -> "4242|short|RUNNING\n")
    ctl = JobController(garbled, _jb_policy(; dry_run=false), outdir)
    ds = manage!(ctl, _jb_work(100, 60000))
    @test only(ds).action === :refuse
    @test occursin("could not be asked", only(ds).reason)
    @test isempty(ctl.ledger.jobs)
    ev = only(_jb_events(outdir))
    @test ev.kind == "job_decision" && ev.action == "refuse"
end

@testset "cli jobs: a profile with max_key_time and no cost model is said, not thrown (#106)" begin
    dir = mktempdir()
    try
        cp(_JB_CFG, joinpath(dir, "study.toml"))
        meta = joinpath(dir, "campaign.toml")
        write(
            meta,
            """
            [campaign]
            name = "c"
            outdir = "$(joinpath(dir, "out"))"

            [[study]]
            name = "pm"
            stages = { phase1 = "study.toml" }

            [profile.short]
            max_key_time = "20min"

            [jobs]
            name = "c"
            budget_node_hours = 5

            [[jobs.partition]]
            name = "p"
            nodes = 1
            time_limit = "30min"
            script = "/abs/run.sh"
            profile = "short"
            """,
        )
        saved = SweepRunner._CLI_SCHEDULER[]
        SweepRunner._CLI_SCHEDULER[] = () -> MockScheduler()
        try
            io = IOBuffer()
            @test SweepRunner.cli(["jobs", meta]; io=io) == 2
            text = String(take!(io))
            @test occursin("max_key_time", text) && occursin("no cost model", text)
        finally
            SweepRunner._CLI_SCHEDULER[] = saved
        end
    finally
        rm(dir; recursive=true, force=true)
    end
end
