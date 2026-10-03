# Dispatch that knows about the other masters (#77) and about cost and the wall clock (#74).

using SweepRunner, Test, DataVault, ParamIO, JSON3

const _DP_CFG = joinpath(@__DIR__, "fixtures", "affinity.toml")      # 24 keys

function _dp_vault(f; run="dp")
    outdir = mktempdir()
    try
        f(DataVault.Vault(_DP_CFG; run=run, outdir=outdir), outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

function _dp_events(outdir)
    logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir(outdir))
    return [JSON3.read(l) for f in logs for l in readlines(joinpath(outdir, f))]
end

_dp_quiet(; kw...) = RunOpts(; status_interval=0, control_interval=0, shard=nothing, kw...)

# `work_fn` recording the order keys ran in.
_dp_rec(order) = k -> (push!(order, ParamIO.canonical(k)); Dict{String,Any}("x" => 1))

@testset "RunOpts: order and shard are checked, and the shard can come from the environment" begin
    @test_throws ArgumentError RunOpts(; order=:random)
    @test_throws ArgumentError RunOpts(; shard=(2, 2))
    @test_throws ArgumentError RunOpts(; shard=(-1, 2))
    @test_throws ArgumentError RunOpts(; shard=(0, 0))
    @test RunOpts(; shard=(1, 4)).shard == (1, 4)
    none = ("SWEEPRUNNER_SHARD" => nothing, "SLURM_ARRAY_TASK_ID" => nothing)
    withenv(none...) do
        @test RunOpts().shard === nothing
    end
    withenv("SWEEPRUNNER_SHARD" => "2/5") do
        @test RunOpts().shard == (2, 5)
        @test RunOpts(; shard=nothing).shard === nothing          # said explicitly: none
    end
    withenv("SWEEPRUNNER_SHARD" => "nonsense") do
        @test RunOpts().shard === nothing
    end
    # A Slurm array task is one of its array, counted from the array's first index.
    withenv(
        "SWEEPRUNNER_SHARD" => nothing,
        "SLURM_ARRAY_TASK_ID" => "3",
        "SLURM_ARRAY_TASK_COUNT" => "4",
        "SLURM_ARRAY_TASK_MIN" => "1",
    ) do
        @test RunOpts().shard == (2, 4)
    end
end

@testset "RunOpts: the numbers are checked when the options are built (#110)" begin
    @test_throws ArgumentError RunOpts(; max_attempts=0)
    @test_throws ArgumentError RunOpts(; heartbeat_interval=0)
    @test_throws ArgumentError RunOpts(; stale_after=-1)
    @test_throws ArgumentError RunOpts(; defer_poll=0)
    @test_throws ArgumentError RunOpts(; defer_poll=NaN)
    for f in (
        :status_interval,
        :control_interval,
        :manifest_interval,
        :checkpoint_every,
        :idle_grace,
        :stop_grace,
    )
        @test_throws ArgumentError RunOpts(; f => -1.0)
        @test_throws ArgumentError RunOpts(; f => NaN)
        @test getfield(RunOpts(; f => 0), f) == 0.0              # 0 is "off", and allowed
    end
    @test_throws ArgumentError RunOpts(; min_busy_fraction=1.5)
    @test_throws ArgumentError RunOpts(; min_busy_fraction=-0.1)
    @test RunOpts(; min_busy_fraction=1).min_busy_fraction == 1.0
    @test_throws ArgumentError RunOpts(; workers=:threads)
    @test_throws ArgumentError RunOpts(; log_level=:loud)
end

@testset "RunOpts: deadline is a point in time, deadline_in the seconds until it (#110)" begin
    t0 = time()
    o = RunOpts(; deadline_in=3600)
    @test t0 + 3600 <= o.deadline <= time() + 3600
    @test_throws ArgumentError RunOpts(; deadline=time() + 10, deadline_in=10)
    @test_throws ArgumentError RunOpts(; deadline_in=-1)
    @test_throws ArgumentError RunOpts(; deadline=NaN)
    # A duration written where the point in time goes is accepted, as before, and said.
    o = @test_logs (:warn, r"deadline_in = 3600") RunOpts(; deadline=3600)
    @test o.deadline == 3600.0
    @test_logs RunOpts(; deadline=time() - 1)                   # past, but a time: not a slip
end

@testset "_shard_of: every master agrees, and the shares are of similar size" begin
    ks = ["N=$(i);J=0.$(i);#sample=1" for i in 1:400]
    for m in (1, 2, 3, 7)
        s = [SweepRunner._shard_of(k, m) for k in ks]
        @test all(x -> 0 <= x < m, s)
        @test s == [SweepRunner._shard_of(k, m) for k in ks]       # the same answer again
        for i in 0:(m - 1)
            @test count(==(i), s) > 400 / m / 2
        end
    end
    # A fixed point, so a change of the hash is noticed: masters on different versions must agree.
    @test SweepRunner._shard_of("N=4;J=0.5;#sample=1", 1000) == 29
end

@testset "shard: a master starts on its own share and still covers every key" begin
    _dp_vault() do v, _
        ks = DataVault.keys(v)
        share =
            i -> Set(
                ParamIO.canonical(k) for
                k in ks if SweepRunner._shard_of(ParamIO.canonical(k), 2) == i
            )
        @test !isempty(share(0)) && !isempty(share(1))
        order = String[]
        r = run!(_dp_rec(order), v, ks; opts=_dp_quiet(; shard=(1, 2)))
        @test r.done == length(ks)                      # all of them, not only its share
        n = length(share(1))
        @test Set(order[1:n]) == share(1)               # its share first
        @test Set(order[(n + 1):end]) == share(0)
        # Within each part the caller's order is kept.
        pos = Dict(ParamIO.canonical(k) => i for (i, k) in enumerate(ks))
        @test issorted([pos[k] for k in order[1:n]])
        @test issorted([pos[k] for k in order[(n + 1):end]])
    end
end

@testset "two masters sharing a sweep collide less when they are sharded" begin
    function collisions(shards)
        total = Ref(0)
        _dp_vault() do v, _
            ks = DataVault.keys(v)
            work = k -> (sleep(0.05); Dict{String,Any}("x" => 1))
            ts = [
                Threads.@spawn run!(work, v, ks; opts=_dp_quiet(; shard=s)) for s in shards
            ]
            rs = fetch.(ts)
            @test all(k -> DataVault.is_done(v, k), ks)
            @test sum(r.done for r in rs) == length(ks)             # each key exactly once
            total[] = sum(r.collisions for r in rs)
        end
        return total[]
    end
    plain = collisions([nothing, nothing])
    sharded = collisions([(0, 2), (1, 2)])
    @info "collisions between two masters" plain sharded
    @test sharded <= plain
end

@testset "collisions are counted: a key handed out that someone else had taken" begin
    _dp_vault() do v, outdir
        ks = DataVault.keys(v)[1:3]
        sib = owner_token()
        # While the first key runs, a sibling takes the second: the scan did not see it.
        work = k -> begin
            k == ks[1] && DataVault.acquire_running!(v, ks[2], sib)
            return Dict{String,Any}("x" => 1)
        end
        r = run!(work, v, ks; opts=_dp_quiet())
        @test r.collisions == 1
        @test (r.done, r.busy) == (2, 1)
        done = only([e for e in _dp_events(outdir) if e.kind == "stage_done"])
        @test done.collisions == 1
        DataVault.clear_running!(v, ks[2], sib)
    end
end

@testset "order=:longest_first draws by cost, and keeps the caller's order among equals" begin
    _dp_vault() do v, _
        ks = DataVault.keys(v)
        cost = k -> Float64(ParamIO.param(k, "L"))                   # two classes: 8 and 16
        order = String[]
        r = run!(_dp_rec(order), v, ks; opts=_dp_quiet(; order=:longest_first), cost=cost)
        @test r.done == length(ks)
        by = Dict(ParamIO.canonical(k) => k for k in ks)
        costs = [cost(by[k]) for k in order]
        @test issorted(costs; rev=true)
        pos = Dict(ParamIO.canonical(k) => i for (i, k) in enumerate(ks))
        long = [pos[k] for k in order if cost(by[k]) == 16]
        @test issorted(long)
        # Without a cost there is nothing to order by: the caller's order.
        order2 = String[]
        _dp_vault(; run="dp2") do v2, _
            run!(
                _dp_rec(order2),
                v2,
                DataVault.keys(v2);
                opts=_dp_quiet(; order=:longest_first),
            )
        end
        @test order2 == ParamIO.canonical.(ks)
    end
end

@testset "a key that cannot get anywhere before the deadline is not started" begin
    _dp_vault() do v, outdir
        ks = DataVault.keys(v)
        # L=16 keys need 1000 s to their next checkpoint; the job has a minute left.
        need = k -> ParamIO.param(k, "L") == 16 ? 1000.0 : 0.01
        nlong = count(k -> ParamIO.param(k, "L") == 16, ks)
        ran = String[]
        r = run!(_dp_rec(ran), v, ks; opts=_dp_quiet(; deadline=time() + 60), min_time=need)
        @test r.held_back == nlong
        @test r.done == length(ks) - nlong
        @test (r.busy, r.err, r.stop) == (0, 0, 0)
        @test r.remaining == nlong
        @test r.stopped_by === nothing                  # nothing was stopped: they never started
        by = Dict(ParamIO.canonical(k) => k for k in ks)
        @test all(k -> ParamIO.param(by[k], "L") == 8, ran)
        ev = only([e for e in _dp_events(outdir) if e.kind == "held_back"])
        @test ev.keys == nlong
        @test 0 < ev.secs_left <= 60

        # Without a deadline there is nothing they cannot reach.
        r = run!(_dp_rec(ran), v, ks; opts=_dp_quiet(), min_time=need)
        @test (r.done, r.held_back) == (nlong, 0)
    end
end

@testset "min_time defaults to cost, and a key the caller's hook cannot answer for is not assumed to fit" begin
    _dp_vault() do v, _
        ks = DataVault.keys(v)
        r = run!(
            _dp_rec(String[]),
            v,
            ks;
            opts=_dp_quiet(; deadline=time() + 60),
            cost=k -> 1000.0,
        )
        @test (r.done, r.held_back) == (0, length(ks))
        # Unknown is not zero: with a deadline, a key the hook has no answer for is held back,
        # and the event says how many were held for that reason.
        _dp_vault(; run="dp3") do v3, outdir3
            ks3 = DataVault.keys(v3)
            r = run!(
                _dp_rec(String[]),
                v3,
                ks3;
                opts=_dp_quiet(; deadline=time() + 60),
                min_time=k -> error("no estimate"),
            )
            @test (r.done, r.held_back) == (0, length(ks3))
            ev = only([e for e in _dp_events(outdir3) if e.kind == "held_back"])
            @test ev.cost_unknown == length(ks3)
            # Without a deadline there is nothing to fit, and they run.
            r = run!(
                _dp_rec(String[]), v3, ks3; opts=_dp_quiet(), min_time=k -> error("no")
            )
            @test r.done == length(ks3)
        end
    end
end

@testset "run_loop! does not sit out idle rounds over keys that cannot fit" begin
    _dp_vault() do v, _
        ks = DataVault.keys(v)
        need = k -> ParamIO.param(k, "L") == 16 ? 1000.0 : 0.01
        t0 = time()
        r = run_loop!(
            _dp_rec(String[]),
            v,
            ks;
            opts=_dp_quiet(; deadline=time() + 60),
            min_time=need,
            idle_sleep=30.0,
        )
        @test time() - t0 < 25
        @test r.stopped_by === :deadline
        @test r.done == count(k -> ParamIO.param(k, "L") == 8, ks)
    end
end
