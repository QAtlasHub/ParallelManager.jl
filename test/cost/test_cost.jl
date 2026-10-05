# Cost (#73): what a key cost is recorded, summarised per class, and readable by the next job.

using SweepRunner, Test, DataVault, ParamIO, JSON3

const _CO_CFG = joinpath(@__DIR__, "..", "run", "fixtures", "study.toml")

function _co_vault(f; run="co")
    outdir = mktempdir()
    try
        f(DataVault.Vault(_CO_CFG; run=run, outdir=outdir), outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

const _CO_N = Ref(0)

# One finished attempt of its own key (every call is a different key).
function _co_cost(class, wall; cpu=wall, cores=1, rss=1000, key=nothing, outcome="ok")
    k = key === nothing ? "k$(_CO_N[] += 1)" : key
    return KeyCost(
        "s", k, class, wall, cpu, cores, rss, "h", 1, Dict{String,Any}(), outcome, "key"
    )
end

# Burn CPU for about `secs` and hold `mb` MiB, so both show up in the record.
function _co_burn(secs, mb)
    buf = fill(0x01, mb * 2^20)
    t0 = time()
    x = 0.0
    while time() - t0 < secs
        for i in 1:10_000
            x += sqrt(i)
        end
    end
    return sum(buf) + x
end

@testset "key_done carries what the key cost" begin
    _co_vault() do v, outdir
        ks = DataVault.keys(v)
        work = k -> begin
            _co_burn(0.3, 64)
            @test note_key!(; segments=3, label="x") == true
            return Dict{String,Any}("x" => 1)
        end
        r = run!(work, v, ks; key_class=k -> "N=$(k.params["N"])")
        @test r.done == length(ks)

        cs = key_costs(v)
        @test length(cs) == length(ks)
        @test Set(c.key for c in cs) == Set(ParamIO.canonical.(ks))
        @test Set(c.class for c in cs) == Set("N=$(k.params["N"])" for k in ks)
        for c in cs
            @test c.stage == "co"
            @test c.wall >= 0.3
            @test c.cpu >= 0.2                      # it was computing, not sleeping
            @test c.cpu <= c.wall * max(c.cores, 1) * 1.5 + 1
            @test c.cores >= 1
            @test c.rss >= 64 * 2^20                # at least what it held
            @test c.host == gethostname()
            @test c.attempt == 1
            @test c.note == Dict("segments" => 3, "label" => "x")
        end
        # By outdir, all stages or one.
        @test length(key_costs(outdir)) == length(ks)
        @test isempty(key_costs(outdir; stage="other"))
    end
end

@testset "without a key_class the class is empty, and a throwing one costs only the label" begin
    @test note_key!(; a=1) == false                  # outside a run!
    _co_vault() do v, _
        ks = DataVault.keys(v)
        run!(k -> Dict{String,Any}("x" => 1), v, ks[1:1])
        run!(k -> Dict{String,Any}("x" => 1), v, ks[2:2]; key_class=k -> error("no"))
        cs = key_costs(v)
        @test length(cs) == 2
        @test all(c -> c.class == "", cs)
        @test all(c -> isempty(c.note), cs)
    end
end

@testset "cost_summary: per class, medians and the peak" begin
    cs = [
        _co_cost("a", 10.0; cpu=8.0, cores=2, rss=100),
        _co_cost("a", 20.0; cpu=20.0, cores=2, rss=300),
        _co_cost("a", 30.0; cpu=60.0, cores=2, rss=200),
        _co_cost("b", 5.0; cpu=NaN, cores=0, rss=0),         # a record from before the measurement
    ]
    s = cost_summary(cs)
    @test Set(keys(s)) == Set(["a", "b"])
    a = s["a"]
    @test (a.n, a.wall_median, a.wall_p90, a.cpu_median) == (3, 20.0, 30.0, 20.0)
    @test a.cores == 2
    @test a.rss_peak == 300
    @test a.efficiency ≈ 0.5                         # 0.4, 0.5, 1.0 -> median 0.5
    b = s["b"]
    @test b.n == 1 && b.wall_median == 5.0
    @test isnan(b.cpu_median) && isnan(b.efficiency)
    # Any grouping: by host, here.
    @test only(keys(cost_summary(cs; by=c -> c.host))) == "h"
    @test isempty(cost_summary(KeyCost[]))
end

@testset "the table a job leaves is what the next one estimates from" begin
    _co_vault() do v, outdir
        ks = DataVault.keys(v)
        class = k -> "N=$(k.params["N"])"
        @test isempty(load_cost_table(v))
        # run_loop! writes the table when it ends.
        r = run_loop!(
            k -> (sleep(k.params["N"] == 8 ? 0.4 : 0.1); Dict{String,Any}("x" => 1)),
            v,
            [k for k in ks if k.params["N"] in (4, 8)];
            key_class=class,
        )
        @test r.done > 0
        @test isfile(SweepRunner.cost_table_path(v))
        t = load_cost_table(v)
        @test Set(keys(t)) == Set(["N=4", "N=8"])
        @test t["N=8"].wall_median > t["N=4"].wall_median
        @test t["N=4"].n == count(k -> k.params["N"] == 4, ks)

        est = measured_cost(t, class; fallback=k -> 999.0)
        k4 = first(k for k in ks if k.params["N"] == 4)
        @test est(k4) == t["N=4"].wall_median
        @test measured_cost(t, class; quantile=:p90)(k4) == t["N=4"].wall_p90
        # A class not measured yet falls back to the application's estimate.
        unseen = ParamIO.DataKey(Dict{String,Any}("N" => 64, "J" => 1.0), 1)
        @test est(unseen) == 999.0
        @test isnan(measured_cost(t, class)(unseen))
        @test_throws ArgumentError measured_cost(t, class; quantile=:max)

        mem = measured_mem(t, class; margin=1.5, fallback=k -> 7.0)
        @test mem(k4) == t["N=4"].rss_peak * 1.5
        @test mem(unseen) == 7.0

        text = sprint(io -> print_costs(io, v))
        @test occursin("N=4", text) && occursin("median s", text)
        @test SweepRunner.cli(["costs", outdir]; io=IOBuffer()) == 0
        @test sprint(io -> print_costs(io, mktempdir())) == "no finished keys on record\n"

        # A table that cannot be read is an empty table, not an error.
        write(SweepRunner.cost_table_path(v), "{ not json")
        @test isempty(load_cost_table(v))
    end
end

@testset "cost_summary: a key's time is the sum over its attempts; unfinished keys are counted" begin
    cs = [
        # One key: two legs that did not finish it, then the one that did.
        _co_cost("a", 100.0; key="x", outcome="stopped"),
        _co_cost("a", 50.0; key="x", outcome="error"),
        _co_cost("a", 30.0; key="x"),
        _co_cost("a", 20.0; key="y"),
        # A key that never finished: killed for memory twice.
        _co_cost("a", 5.0; key="z", outcome="worker_died", rss=9000),
        _co_cost("a", 6.0; key="z", outcome="worker_died", rss=9500),
    ]
    a = cost_summary(cs)["a"]
    @test a.n == 2                                   # x and y finished
    @test a.wall_p90 == 180.0                        # x took 100 + 50 + 30, not its last 30
    @test a.wall_median == 20.0
    @test a.unfinished == 1                          # z
    @test a.rss_peak == 9500                         # the peak of the attempts that died counts
    @test a.rss_process == false
    only_dead = cost_summary([_co_cost("b", 5.0; outcome="worker_died")])["b"]
    @test only_dead.n == 0 && isnan(only_dead.wall_median) && only_dead.unfinished == 1
end

@testset "every attempt leaves a record, with its outcome (#106)" begin
    _co_vault() do v, _
        k = DataVault.keys(v)[1]
        n = Ref(0)
        work = key -> begin
            n[] += 1
            sleep(0.3)
            n[] == 1 && error("first leg fails")
            return Dict{String,Any}("x" => 1)
        end
        r = run!(work, v, [k]; opts=RunOpts(; max_attempts=2), key_class=key -> "c")
        @test r.done == 1
        cs = key_costs(v)
        @test [c.outcome for c in sort(cs; by=c -> c.attempt)] == ["error", "ok"]
        @test all(c -> c.class == "c" && c.wall >= 0.3 && c.rss > 0, cs)
        @test all(c -> c.rss_scope in ("key", "process"), cs)
        # The class's time is both legs.
        @test cost_summary(cs)["c"].wall_median >= 0.6
        @test cost_summary(cs)["c"].n == 1
    end
    # A key that never finishes is on record too, as unfinished.
    _co_vault() do v, _
        k = DataVault.keys(v)[1]
        run!(
            key -> error("always"),
            v,
            [k];
            opts=RunOpts(; max_attempts=2),
            key_class=key -> "c",
        )
        s = cost_summary(key_costs(v))["c"]
        @test (s.n, s.unfinished) == (0, 1)
        write_cost_table(v)
        @test isempty(load_cost_table(v))            # nothing to estimate from
    end
end

@testset "the measured table is the default cost, and the fallback is said (#106)" begin
    _co_vault() do v, outdir
        ks = DataVault.keys(v)
        class = k -> "N=$(k.params["N"])"
        n4 = [k for k in ks if k.params["N"] == 4]
        n8 = [k for k in ks if k.params["N"] == 8]
        run_loop!(k -> (sleep(0.05); Dict{String,Any}("x" => 1)), v, n4; key_class=class)
        @test collect(keys(load_cost_table(v))) == ["N=4"]
        # The next round finds the table by itself; N=8 has not been seen.
        run!(k -> Dict{String,Any}("x" => 1), v, n8; key_class=class, cost=k -> 7.0)
        logs = filter(f -> startswith(f, "events_"), readdir(outdir))
        ev = [JSON3.read(l) for f in logs for l in readlines(joinpath(outdir, f))]
        src = only([e for e in ev if e.kind == "cost_source"])
        @test src.source == "measured"
        @test (src.measured_keys, src.fallback_keys) == (0, length(n8))
        @test src.fallback == "the caller's cost"
        # A table that cannot be read is said, and the run goes on with the caller's hook.
        write(SweepRunner.cost_table_path(v), "{ not json")
        @test_throws Exception load_cost_table(v; strict=true)
        v2 = DataVault.Vault(_CO_CFG; run="co2", outdir=outdir)
        mkpath(dirname(SweepRunner.cost_table_path(v2)))
        write(SweepRunner.cost_table_path(v2), "{ not json")
        r = run!(k -> Dict{String,Any}("x" => 1), v2, DataVault.keys(v2); key_class=class)
        @test r.done == length(ks)
        ev = [JSON3.read(l) for f in logs for l in readlines(joinpath(outdir, f))]
        @test any(e -> e.kind == "cost_table_unreadable", ev)
    end
end

@testset "the table is kept while the round runs, not only at a clean end (#106)" begin
    _co_vault() do v, _
        ks = DataVault.keys(v)
        seen = Bool[]
        work = k -> begin
            push!(seen, haskey(load_cost_table(v), "c"))
            sleep(0.05)
            return Dict{String,Any}("x" => 1)
        end
        run!(work, v, ks; opts=RunOpts(; manifest_interval=0.01), key_class=k -> "c")
        # A job killed after its second key would have left what the first one cost.
        @test seen[1] == false
        @test seen[end] == true
    end
end

@testset "key_seconds: one guarded way to ask a cost hook" begin
    k = ParamIO.DataKey(Dict{String,Any}("N" => 1), 1)
    @test key_seconds(x -> 12, k) == 12.0
    @test key_seconds(x -> NaN, k) === nothing
    @test key_seconds(x -> Inf, k) === nothing
    @test key_seconds(x -> -1.0, k) === nothing
    @test key_seconds(x -> error("no model"), k) === nothing
    @test key_seconds(x -> "soon", k) === nothing
end

# ── a round's end does not read the outdir (#198) ────────────────────────────────────────────────

function _co_events(outdir)
    return [
        JSON3.read(l) for f in readdir(outdir) if startswith(f, "events_") &&
            endswith(f, ".jsonl") &&
            filesize(joinpath(outdir, f)) > 0 &&
            (stat(joinpath(outdir, f)).mode & 0o400) != 0 for
        l in readlines(joinpath(outdir, f))
    ]
end

# Event files of other processes and other jobs, as a long campaign leaves them — and not
# readable, so that anything that opens them fails.
function _co_foreign_logs(outdir, n)
    paths = [joinpath(outdir, "events_other_$i.jsonl") for i in 1:n]
    for p in paths
        write(p, "{\"kind\":\"key_done\",\"stage\":\"co\",\"key\":\"old\",\"secs\":99}\n")
        chmod(p, 0o000)
    end
    return paths
end

@testset "a run! that nobody reads a cost table from does not keep one, and reads no event file (#198)" begin
    _co_vault() do v, outdir
        ks = DataVault.keys(v)
        foreign = _co_foreign_logs(outdir, 50)
        try
            # One key per `run!`, on this process, with no class and no pool: the shape of a
            # caller that drives its own workers. Under 0.6.9 each of these reread the outdir.
            for k in ks
                r = run!(
                    k -> Dict{String,Any}("x" => 1),
                    v,
                    [k];
                    opts=RunOpts(; workers=:sequential),
                )
                @test r.done == 1
            end
            @test !isfile(SweepRunner.cost_table_path(v))
            @test !isfile(SweepRunner.cost_records_path(v))
            @test !any(
                e -> e.kind in ("cost_table_failed", "cost_table_not_seeded"),
                _co_events(outdir),
            )
        finally
            foreach(p -> chmod(p, 0o600), foreign)
        end
    end
end

@testset "a round keeps the table from its own records, whatever else is under the outdir (#198)" begin
    _co_vault() do v, outdir
        ks = DataVault.keys(v)
        class = k -> "N=$(k.params["N"])"
        SweepRunner._COST_SEED_MAX_FILES[] = 20
        foreign = _co_foreign_logs(outdir, 50)                   # more than it will seed from
        try
            r = run!(
                k -> Dict{String,Any}("x" => 1),
                v,
                ks[1:2];
                key_class=class,
                opts=RunOpts(; workers=:sequential),
            )
            @test r.done == 2
            # Not one of the fifty was opened: the table is there, nothing failed, and the
            # history that was not read is said once.
            ev = _co_events(outdir)
            @test !any(e -> e.kind == "cost_table_failed", ev)
            said = only([e for e in ev if e.kind == "cost_table_not_seeded"])
            @test said.event_files >= 50 && said.max == 20
            @test sum(c.n for c in values(load_cost_table(v))) == 2
            records = readlines(SweepRunner.cost_records_path(v))
            @test length(records) == 2
            # The next round adds its own records to the same file and reads only those.
            r = run!(
                k -> Dict{String,Any}("x" => 1),
                v,
                ks[3:end];
                key_class=class,
                opts=RunOpts(; workers=:sequential),
            )
            @test r.done == length(ks) - 2
            @test length(readlines(SweepRunner.cost_records_path(v))) == length(ks)
            @test sum(c.n for c in values(load_cost_table(v))) == length(ks)
            @test count(e -> e.kind == "cost_table_not_seeded", _co_events(outdir)) == 1
        finally
            SweepRunner._COST_SEED_MAX_FILES[] = 200
            foreach(p -> chmod(p, 0o600), foreign)
        end
        # The explicit rebuild is the full rescan: it brings in what the fifty hold.
        t = write_cost_table(v)
        @test sum(c.n for c in values(t)) == length(ks) + 1      # the fifty all name one old key
        @test length(readlines(SweepRunner.cost_records_path(v))) == length(ks) + 50
    end
end

@testset "a stage with a short history is seeded from it once; cost_table says when to keep one (#198)" begin
    _co_vault() do v, outdir
        ks = DataVault.keys(v)
        class = k -> "N=$(k.params["N"])"
        # An earlier job of this stage, under a version without the record file.
        old = joinpath(outdir, "events_earlier_1.jsonl")
        write(
            old,
            """{"kind":"key_done","stage":"co","key":"old","class":"N=4","secs":5.0,"cores":1,"rss":1}\n""",
        )
        run!(
            k -> Dict{String,Any}("x" => 1),
            v,
            ks[1:1];
            key_class=class,
            opts=RunOpts(; workers=:sequential),
        )
        @test sum(c.n for c in values(load_cost_table(v))) == 2  # the old key and the new one
        @test !any(e -> e.kind == "cost_table_not_seeded", _co_events(outdir))
        # A record that is both in the seed and appended later counts once.
        line = first(readlines(SweepRunner.cost_records_path(v)))
        open(io -> println(io, line), SweepRunner.cost_records_path(v), "a")
        run!(
            k -> Dict{String,Any}("x" => 1),
            v,
            ks[2:2];
            key_class=class,
            opts=RunOpts(; workers=:sequential),
        )
        @test sum(c.n for c in values(load_cost_table(v))) == 3
    end
    _co_vault() do v, outdir
        ks = DataVault.keys(v)
        seq = RunOpts(; workers=:sequential)
        wf = k -> Dict{String,Any}("x" => 1)
        # Asked for without a class; refused with one; and only those three values.
        run!(wf, v, ks[1:1]; cost_table=true, opts=seq)
        @test isfile(SweepRunner.cost_table_path(v))
        rm(SweepRunner.cost_table_path(v))
        run!(wf, v, ks[2:2]; cost_table=false, key_class=k -> "c", opts=seq)
        @test !isfile(SweepRunner.cost_table_path(v))
        @test_throws ArgumentError run!(wf, v, ks[3:3]; cost_table=:always, opts=seq)
        r = run_loop!(
            wf, v, ks[3:3]; cost_table=true, opts=seq, max_empty_rounds=1, idle_sleep=0
        )
        @test r.done == 1 && isfile(SweepRunner.cost_table_path(v))
    end
end

@testset "a round of a few keys reads those keys' progress stamps, not the stage's (#198)" begin
    _co_vault() do v, outdir
        ks = DataVault.keys(v)
        # Stamps of other units of the stage, partly done somewhere else — and damaged, so that
        # reading them is noticed.
        dir = SweepRunner.progress_dir(v)
        mkpath(dir)
        foreach(i -> write(joinpath(dir, "other$i.json"), "{ not json"), 1:5)
        seq = RunOpts(; workers=:sequential)
        r = run!(k -> Dict{String,Any}("x" => 1), v, ks[1:1]; opts=seq)
        @test r.done == 1
        @test !any(e -> e.kind == "progress_unreadable", _co_events(outdir))
        # Asked about the whole stage, they are read, and said.
        lost = Ref(0)
        read_progress(v; unreadable=lost)
        @test lost[] == 5
        # A round's own key that left a stamp still gets its resume point.
        @test SweepRunner._PROGRESS_LIST_ABOVE >= 2             # this round is a "few keys" one
        k = ks[2]
        got = Ref{Any}(nothing)
        stamp = Dict("key" => ParamIO.canonical(k), "step" => 3, "of" => 10, "at" => time())
        write(SweepRunner._progress_file(v, ParamIO.canonical(k)), JSON3.write(stamp))
        run!(v, [k]; opts=seq) do key
            got[] = SweepRunner.resume_point()
            return Dict{String,Any}("x" => 1)
        end
        @test got[].step == 3
    end
end
