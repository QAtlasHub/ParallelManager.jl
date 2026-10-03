# Campaign (#67): a meta config names the configs a campaign runs, their order and their filters.

using SweepRunner, Test, DataVault, ParamIO, JSON3
using SweepRunner: parse_duration

_cp_config(project) = """
[study]
project_name  = "$project"
total_samples = 1
outdir        = "out"

[datavault]
path_keys = ["N"]

[[paramsets]]
N = [4, 8]
"""

_cp_meta(out) = """
[campaign]
name   = "t"
outdir = "$out"

[[study]]
name   = "conv"
stages = { phase1 = "conv_phase1.toml", phase2 = "conv_phase2.toml" }

[[study]]
name    = "typ"
stages  = { phase1 = "typ_phase1.toml" }
flavour = "x"

[[study]]
name     = "typx"
stages   = { phase2 = "typx_phase2.toml" }
needs    = ["conv.phase1", "typ"]
priority = 10

[profile.short]
max_key_time = "5s"
skip_stages  = ["conv.phase1"]

[profile.typ_only]
studies   = ["typ"]
min_nodes = 16
"""

# A directory with four stage configs and a meta config; `meta(out)` overrides the meta text.
function _cp_setup(f; meta=_cp_meta)
    dir = mktempdir()
    try
        for (file, project) in (
            ("conv_phase1.toml", "conv"),
            ("conv_phase2.toml", "conv"),
            ("typ_phase1.toml", "typ"),
            ("typx_phase2.toml", "typx"),
        )
            write(joinpath(dir, file), _cp_config(project))
        end
        out = joinpath(dir, "out")
        path = joinpath(dir, "campaign.toml")
        write(path, meta(out))
        f(path, dir, out)
    finally
        rm(dir; recursive=true, force=true)
    end
end

_cp_quiet() = RunOpts(; status_interval=0, control_interval=0)

function _cp_events(out)
    logs = filter(f -> startswith(f, "events_campaign_"), readdir(out))
    return [JSON3.read(l) for f in logs for l in readlines(joinpath(out, f))]
end

# `open_stage` whose work records which stage each key ran under.
function _cp_open(order; hook=nothing)
    return s -> (; work_fn=k -> begin
        push!(order, stage_id(s))
        hook === nothing || hook(s, k)
        Dict{String,Any}("x" => 1)
    end)
end

_cp_errors(r) = [f.message for f in r.findings if f.severity === :error]

@testset "parse_duration" begin
    @test parse_duration(90) == 90.0
    @test parse_duration("90s") == 90.0
    @test parse_duration("20min") == 1200.0
    @test parse_duration("2 h") == 7200.0
    @test parse_duration("1.5d") == 129600.0
    @test parse_duration("30") == 30.0
    @test_throws ArgumentError parse_duration("soon")
    @test_throws ArgumentError parse_duration("3 fortnights")
    # A duration is finite and not negative (#113).
    @test parse_duration(0) == 0.0
    @test_throws ArgumentError parse_duration(-5)
    @test_throws ArgumentError parse_duration(NaN)
    @test_throws ArgumentError parse_duration(Inf)
    @test_throws ArgumentError parse_duration("-5s")
end

@testset "a profile's numbers are checked, and a campaign that cannot launch has no work to count (#113)" begin
    bad =
        out -> replace(
            _cp_meta(out),
            "max_key_time = \"5s\"" => "max_key_time = -5",
            "min_nodes = 16" => "min_nodes = 0",
        )
    _cp_setup(; meta=bad) do path, _, _
        c = load_campaign(path)
        errs = _cp_errors(validate_campaign(c))
        @test any(m -> occursin("not a duration", m), errs)
        @test any(m -> occursin("min_nodes must be an integer >= 1", m), errs)
        @test_throws ArgumentError remaining_work(_cp_open(String[]), c)
        @test_throws ArgumentError campaign_work(_cp_open(String[]), c)("short")
    end
end

@testset "run_campaign!: the job's profile comes from its environment, and is on record (#113)" begin
    _cp_setup() do path, _, out
        c = load_campaign(path)
        order = String[]
        withenv("SWEEPRUNNER_PROFILE" => "typ_only", "SLURM_JOB_NUM_NODES" => "4") do
            r = run_campaign!(_cp_open(order), c; opts=_cp_quiet())
            @test r.profile == "typ_only"
            @test unique(order) == ["typ.phase1"]              # the profile's studies only
        end
        ev = _cp_events(out)
        start = only([e for e in ev if e.kind == "campaign_start"])
        @test (start.profile, start.profile_source) == ("typ_only", "SWEEPRUNNER_PROFILE")
        # The profile is for jobs of 16 nodes; this one has 4. Said, not refused.
        small = only([e for e in ev if e.kind == "profile_too_small"])
        @test (small.min_nodes, small.nodes) == (16, 4)
    end
    _cp_setup() do path, _, out
        c = load_campaign(path)
        withenv("SWEEPRUNNER_PROFILE" => "typ_only", "SLURM_JOB_NUM_NODES" => nothing) do
            # An explicit profile wins over the environment.
            r = run_campaign!(
                _cp_open(String[]), c; profile="short", cost=(s, k) -> 1.0, opts=_cp_quiet()
            )
            @test r.profile == "short"
        end
        start = only([e for e in _cp_events(out) if e.kind == "campaign_start"])
        @test start.profile_source == "argument"
        withenv("SWEEPRUNNER_PROFILE" => "no_such_profile") do
            @test_throws ArgumentError run_campaign!(
                _cp_open(String[]), c; opts=_cp_quiet()
            )
        end
    end
end

@testset "load_campaign: studies, stages in order, needs resolved" begin
    _cp_setup() do path, dir, out
        c = load_campaign(path)
        @test c.name == "t"
        @test c.outdir == out
        @test c.studies == ["conv", "typ", "typx"]
        @test all(values(c.enabled))
        @test stage_id.(c.stages) ==
            ["conv.phase1", "conv.phase2", "typ.phase1", "typx.phase2"]
        by = Dict(stage_id(s) => s for s in c.stages)
        @test by["conv.phase1"].needs == String[]
        @test by["conv.phase2"].needs == ["conv.phase1"]            # the chain within a study
        @test by["typx.phase2"].needs == ["conv.phase1", "typ.phase1"]   # "typ" = all its stages
        @test by["typx.phase2"].priority == 10
        @test by["typ.phase1"].extra == Dict("flavour" => "x")      # passed through
        @test by["conv.phase1"].config == joinpath(dir, "conv_phase1.toml")
        @test c.profiles["short"].max_key_time == 5.0
        @test c.profiles["short"].skip_stages == ["conv.phase1"]
        @test c.profiles["typ_only"].studies == ["typ"]
        @test c.profiles["typ_only"].min_nodes == 16
        @test length(c.sha256) == 64
        @test isempty(c.problems)
        @test launchable(validate_campaign(c))
        @test occursin("study typx  priority 10", sprint(show, c))
    end
end

@testset "load_campaign: a table of stages runs in name order, an array as written" begin
    meta = out -> """
    [campaign]
    name = "o"
    outdir = "$out"

    [[study]]
    name = "a"
    stages = { phase10 = "conv_phase2.toml", phase2 = "conv_phase1.toml" }

    [[study]]
    name = "b"
    chain = false
    stages = [
        { name = "late",  config = "typx_phase2.toml", needs = ["a.phase10"] },
        { name = "early", config = "typ_phase1.toml" },
    ]
    """
    _cp_setup(; meta) do path, _, _
        c = load_campaign(path)
        @test stage_id.(c.stages) == ["a.phase2", "a.phase10", "b.late", "b.early"]
        by = Dict(stage_id(s) => s for s in c.stages)
        @test by["a.phase10"].needs == ["a.phase2"]
        @test by["b.late"].needs == ["a.phase10"]
        @test by["b.early"].needs == String[]                      # chain = false
        @test stage_id.(plan_campaign(c)) == ["a.phase2", "a.phase10", "b.late", "b.early"]
    end
end

@testset "plan_campaign: needs first, then priority, then file order" begin
    _cp_setup() do path, _, _
        c = load_campaign(path)
        # typx is priority 10, and what it needs is pulled forward with it.
        @test stage_id.(plan_campaign(c)) ==
            ["conv.phase1", "typ.phase1", "typx.phase2", "conv.phase2"]
        @test stage_id.(plan_campaign(c; studies=["conv"])) ==
            ["conv.phase1", "conv.phase2"]
        @test stage_id.(plan_campaign(c; profile="typ_only")) == ["typ.phase1"]
        @test stage_id.(plan_campaign(c; profile=:short)) ==
            ["typ.phase1", "typx.phase2", "conv.phase2"]
        # An explicit selection outranks the profile's.
        @test stage_id.(plan_campaign(c; profile="typ_only", studies=["typx"])) ==
            ["typx.phase2"]
        @test_throws ArgumentError plan_campaign(c; profile="nope")
        @test_throws ArgumentError plan_campaign(c; studies=["nope"])
    end
    disabled = out -> replace(_cp_meta(out), "flavour = \"x\"" => "enabled = false")
    _cp_setup(; meta=disabled) do path, _, _
        c = load_campaign(path)
        @test c.enabled["typ"] == false
        @test stage_id.(plan_campaign(c)) == ["conv.phase1", "typx.phase2", "conv.phase2"]
    end
end

@testset "validate_campaign: what is wrong is found before anything runs" begin
    function report(text)
        r = Ref{Any}(nothing)
        _cp_setup(; meta=out -> replace(text, "OUT" => out)) do path, _, _
            r[] = validate_campaign(load_campaign(path))
        end
        return r[]
    end
    head = "[campaign]\nname = \"v\"\noutdir = \"OUT\"\n"

    r = report(head * "[[study]]\nname = \"a\"\nstages = { phase1 = \"missing.toml\" }\n")
    @test !launchable(r)
    @test any(m -> occursin("config not found", m), _cp_errors(r))

    r = report(
        head *
        "[[study]]\nname = \"a\"\nstages = { phase1 = \"conv_phase1.toml\" }\n" *
        "needs = [\"nope.phase1\"]\n",
    )
    @test any(m -> occursin("no stage or study", m), _cp_errors(r))

    r = report(
        head *
        "[[study]]\nname = \"a\"\nneeds = [\"b\"]\nstages = { phase1 = \"conv_phase1.toml\" }\n" *
        "[[study]]\nname = \"b\"\nneeds = [\"a\"]\nstages = { phase1 = \"typ_phase1.toml\" }\n",
    )
    @test any(m -> occursin("cycle", m), _cp_errors(r))

    # Two studies, different configs, the same project and stage name: one record, two writers.
    r = report(
        head *
        "[[study]]\nname = \"a\"\nstages = { phase1 = \"conv_phase1.toml\" }\n" *
        "[[study]]\nname = \"b\"\nstages = { phase1 = \"conv_phase2.toml\" }\n",
    )
    @test any(m -> occursin("writes project conv run phase1", m), _cp_errors(r))

    r = report(
        head *
        "[[study]]\nstages = { phase1 = \"conv_phase1.toml\" }\n" *
        "[[study]]\nname = \"a\"\nstages = { phase1 = \"conv_phase1.toml\" }\n" *
        "[[study]]\nname = \"a\"\nstages = { phase1 = \"typ_phase1.toml\" }\n",
    )
    @test any(m -> occursin("needs a `name`", m), _cp_errors(r))
    @test any(m -> occursin("named twice", m), _cp_errors(r))

    r = report(head * "[profile.p]\nmax_key_time = \"soon\"\n")
    @test any(m -> occursin("not a duration", m), _cp_errors(r))
    @test any(m -> occursin("no [[study]]", m), _cp_errors(r))

    # A profile that names a stage that does not exist is a warning: still launchable.
    r = report(
        head *
        "[[study]]\nname = \"a\"\nstages = { phase1 = \"conv_phase1.toml\" }\n" *
        "[profile.p]\nskip_stages = [\"phase9\"]\n",
    )
    @test launchable(r)
    @test SweepRunner.n_warns(r) == 1
end

@testset "run_campaign!: the stages run in plan order, and the job is on record" begin
    _cp_setup() do path, dir, out
        c = load_campaign(path)
        order = String[]
        r = run_campaign!(_cp_open(order), c; opts=_cp_quiet())
        ids = ["conv.phase1", "typ.phase1", "typx.phase2", "conv.phase2"]
        @test [x.stage for x in r.stages] == ids
        @test all(x -> x.ran, r.stages)
        @test all(x -> x.result.done == 2, r.stages)
        @test unique(order) == ids
        @test length(order) == 8
        @test r.stopped_by === nothing
        @test r.profile === nothing
        # Each stage is its config opened as run <stage name> under the campaign's outdir.
        v = DataVault.Vault(joinpath(dir, "typx_phase2.toml"); run="phase2", outdir=out)
        @test all(k -> DataVault.is_done(v, k), DataVault.keys(v))

        ev = _cp_events(out)
        start = only([e for e in ev if e.kind == "campaign_start"])
        @test start.meta == path
        @test start.sha256 == c.sha256
        @test collect(start.stages) == ids
        @test [e.stage for e in ev if e.kind == "campaign_stage"] == ids
        @test only([e for e in ev if e.kind == "campaign_done"]).ran == 4

        # Again: everything is done, nothing is recomputed, and no idle round is sat out.
        empty!(order)
        t0 = time()
        r2 = run_campaign!(_cp_open(order), c; opts=_cp_quiet())
        @test isempty(order)
        @test all(x -> x.ran && x.result.done == 0, r2.stages)
        @test time() - t0 < 20
    end
end

@testset "run_campaign!: a stage whose needs are not complete is not started" begin
    _cp_setup() do path, _, out
        c = load_campaign(path)
        order = String[]
        # `short` skips conv.phase1, which typx.phase2 and conv.phase2 need.
        r = run_campaign!(
            _cp_open(order), c; profile="short", cost=(s, k) -> 1.0, opts=_cp_quiet()
        )
        @test [(x.stage, x.ran) for x in r.stages] == [("typ.phase1", true), ("typx.phase2", false), ("conv.phase2", false)]
        @test r.stages[2].reason == "needs conv.phase1"
        @test unique(order) == ["typ.phase1"]
        @test r.profile == "short"
        skipped = [e for e in _cp_events(out) if e.kind == "campaign_stage" && !e.ran]
        @test length(skipped) == 2

        # Once conv.phase1 is there, the same profile runs what needed it.
        run_campaign!(_cp_open(String[]), c; studies=["conv"], opts=_cp_quiet())
        empty!(order)
        r = run_campaign!(
            _cp_open(order), c; profile="short", cost=(s, k) -> 1.0, opts=_cp_quiet()
        )
        @test all(x -> x.ran, r.stages)
        @test unique(order) == ["typx.phase2"]         # typ.phase1 and conv.phase2 were done
    end
end

@testset "run_campaign!: a profile's max_key_time leaves the long keys out" begin
    _cp_setup() do path, _, _
        c = load_campaign(path)
        @test_throws ArgumentError run_campaign!(
            _cp_open(String[]), c; profile="short", opts=_cp_quiet()
        )
        ran = Int[]
        open = s -> (; work_fn=k -> (push!(ran, k.params["N"]); Dict{String,Any}("x" => 1)))
        cost = (s, k) -> k.params["N"] == 8 ? 100.0 : 1.0          # seconds; the limit is 5
        r = run_campaign!(
            open, c; profile="short", studies=["typ"], cost=cost, opts=_cp_quiet()
        )
        @test only(r.stages).keys == 1
        @test ran == [4]
    end
end

@testset "run_campaign!: an edit to the meta file reaches a campaign that is running" begin
    disable_typ =
        path ->
            write(path, replace(read(path, String), "flavour = \"x\"" => "enabled = false"))
    _cp_setup() do path, _, out
        c = load_campaign(path)
        order = String[]
        fired = Ref(false)
        hook = (s, k) -> (fired[] || (fired[]=true; disable_typ(path)))
        r = run_campaign!(_cp_open(order; hook), c; opts=_cp_quiet())
        # typ was switched off while conv.phase1 ran: it is not started, and typx, which needs
        # it, is not either.
        @test [(x.stage, x.ran) for x in r.stages] == [("conv.phase1", true), ("typx.phase2", false), ("conv.phase2", true)]
        @test !("typ.phase1" in order)
        @test any(e -> e.kind == "campaign_reloaded", _cp_events(out))
    end
    _cp_setup() do path, _, _
        c = load_campaign(path)
        order = String[]
        fired = Ref(false)
        hook = (s, k) -> (fired[] || (fired[]=true; disable_typ(path)))
        r = run_campaign!(_cp_open(order; hook), c; opts=_cp_quiet(), reload=false)
        @test length(r.stages) == 4 && all(x -> x.ran, r.stages)
    end
    # An edit that breaks the file is refused, said so once, and the campaign goes on as it was.
    _cp_setup() do path, _, out
        c = load_campaign(path)
        fired = Ref(false)
        hook = (s, k) -> (fired[] || (fired[]=true; write(path, "this is = not [toml")))
        r = run_campaign!(_cp_open(String[]; hook), c; opts=_cp_quiet())
        @test length(r.stages) == 4 && all(x -> x.ran, r.stages)
        @test any(e -> e.kind == "campaign_reload_refused", _cp_events(out))
    end
end

@testset "run_campaign!: a stop ends the campaign, not just the stage" begin
    _cp_setup() do path, dir, out
        c = load_campaign(path)
        flag = joinpath(dir, "STOP")
        order = String[]
        hook = (s, k) -> touch(flag)
        r = run_campaign!(
            _cp_open(order; hook),
            c;
            opts=RunOpts(; status_interval=0, control_interval=0, stop_flag=flag),
        )
        @test r.stopped_by === :flag
        @test [x.stage for x in r.stages] == ["conv.phase1"]
        @test length(order) == 1
        @test only([e for e in _cp_events(out) if e.kind == "campaign_done"]).stopped_by == "flag"
    end
end

@testset "remaining_work: what is left, per stage, and what blocks it" begin
    _cp_setup() do path, _, _
        c = load_campaign(path)
        open = _cp_open(String[])
        cost = (s, k) -> Float64(k.params["N"])
        w = Dict(x.stage => x for x in remaining_work(open, c; cost))
        @test all(x -> (x.total, x.todo, x.eligible) == (2, 2, 2), values(w))
        @test w["conv.phase1"].cost == 12.0
        @test w["conv.phase1"].longest == 8.0
        @test w["conv.phase1"].blocked_by == String[]
        @test w["typx.phase2"].blocked_by == ["conv.phase1", "typ.phase1"]
        @test isnan(first(remaining_work(open, c)).cost)

        run_campaign!(open, c; studies=["conv"], opts=_cp_quiet())
        w = Dict(x.stage => x for x in remaining_work(open, c; cost))
        @test (w["conv.phase1"].todo, w["conv.phase1"].cost) == (0, 0.0)
        @test w["typx.phase2"].blocked_by == ["typ.phase1"]
        @test w["typ.phase1"].todo == 2
        # Under the profile only the keys within max_key_time (5 s) count as eligible.
        ws = remaining_work(open, c; profile="short", cost)
        typ = only([x for x in ws if x.stage == "typ.phase1"])
        @test (typ.todo, typ.eligible, typ.cost) == (2, 1, 4.0)
    end
end

@testset "cli: campaign prints the plan, and fails on a meta that is not launchable" begin
    _cp_setup() do path, _, _
        out = sprint(io -> (@test SweepRunner.cli(["campaign", path]; io=io) == 0))
        @test occursin("1. conv.phase1", out)
        @test occursin("4. conv.phase2", out)
        out = sprint(
            io ->
                (@test SweepRunner.cli(["campaign", path, "--profile", "short"]; io=io) ==
                    0),
        )
        @test occursin("plan (profile short)", out) && occursin("1. typ.phase1", out)
        @test SweepRunner.cli(["campaign", path, "--profile", "nope"]; io=IOBuffer()) == 2
        @test SweepRunner.cli(["campaign", path * ".missing"]; io=IOBuffer()) == 2
        write(path, "[campaign]\nname = \"x\"\n")
        @test SweepRunner.cli(["campaign", path]; io=IOBuffer()) == 1
    end
end
