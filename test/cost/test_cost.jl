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

function _co_cost(class, wall; cpu=wall, cores=1, rss=1000)
    return KeyCost("s", "k", class, wall, cpu, cores, rss, "h", 1, Dict{String,Any}())
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
