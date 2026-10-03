# Control (#65): a running sweep takes requests — add work, cancel, stop, reorder, resize, pause.

using SweepRunner, Test, DataVault, ParamIO, JSON3, Distributed
using SweepRunner: matches, control_dir

const _CT_CFG = joinpath(@__DIR__, "..", "run", "fixtures", "study.toml")
const _CT_BIG = joinpath(@__DIR__, "..", "run", "fixtures", "affinity.toml")

function _ct_vault(f; run="ct", cfg=_CT_CFG)
    outdir = mktempdir()
    try
        f(DataVault.Vault(cfg; run=run, outdir=outdir), outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

function _ct_events(outdir)
    logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir(outdir))
    return [JSON3.read(l) for f in logs for l in readlines(joinpath(outdir, f))]
end

function _ct_workers(f, n)
    nprocs() > 1 && rmprocs(workers())
    addprocs(n; exeflags="--project=$(dirname(Base.active_project()))")
    try
        @everywhere workers() Core.eval(
            Main, :(using SweepRunner, DataVault, ParamIO, Distributed)
        )
        f()
    finally
        nprocs() > 1 && rmprocs(workers())
        note_workers!(; planned=0, launched=0)
    end
end

# Requests are read between keys on the sequential path; this makes every gap a poll.
_ct_opts(; kw...) = RunOpts(; control_interval=0.001, kw...)

_ct_ok(k) = Dict{String,Any}("x" => 1)

# `work_fn` that runs `f()` on the first key only, then behaves like `_ct_ok`.
function _ct_once(f; record=nothing)
    fired = Ref(false)
    return k -> begin
        record === nothing || push!(record, ParamIO.canonical(k))
        if !fired[]
            fired[] = true
            f()
        end
        return _ct_ok(k)
    end
end

@testset "KeyFilter: a predicate that is data" begin
    k = ParamIO.DataKey(Dict{String,Any}("system.N" => 32, "J" => 0.5, "tag" => "a"), 2)
    @test matches(KeyFilter(), k)
    @test isempty(KeyFilter())
    @test matches(KeyFilter(; select=Dict("system.N" => 32)), k)
    @test matches(KeyFilter(; select=Dict("system.N" => [16, 32])), k)
    @test matches(KeyFilter(; select=Dict("system.N" => 32.0)), k)      # numbers by value
    @test matches(KeyFilter(; select=Dict("tag" => "a", "J" => 0.5)), k)
    @test !matches(KeyFilter(; select=Dict("system.N" => 64)), k)
    @test !matches(KeyFilter(; select=Dict("system.N" => 32, "J" => 1.0)), k)   # all must hold
    @test !matches(KeyFilter(; select=Dict("absent" => 1)), k)
    @test matches(KeyFilter(; samples=[1, 2]), k)
    @test !matches(KeyFilter(; samples=3), k)
    @test !matches(KeyFilter(; select=Dict("system.N" => 32), samples=1), k)
end

@testset "control!: a request is one file, and a malformed one is refused by the sender" begin
    _ct_vault() do v, outdir
        ks = DataVault.keys(v)
        @test_throws ArgumentError control!(v, :nonsense)
        @test_throws ArgumentError control!(v, :enqueue)                       # neither
        @test_throws ArgumentError control!(v, :enqueue; keys=ks, config=_CT_CFG)
        @test_throws ArgumentError control!(v, :enqueue; config="/no/such.toml")
        @test_throws ArgumentError control!(v, :resize)
        @test_throws ArgumentError control!(v, :resize; n=-1)
        @test_throws ArgumentError control!(v, :drain)
        @test_throws ArgumentError control!(v, :stop; grace=-1)
        @test isempty(read_requests(v))

        id = control!(
            v, :cancel; select=Dict("N" => [4, 8]), samples=1, running=true, grace=30
        )
        req = only(read_requests(v))
        @test req["id"] == id
        @test req["op"] == "cancel"
        @test req["select"] == Dict("N" => [4, 8])
        @test req["samples"] == [1]
        @test req["running"] == true
        @test req["grace"] == 30.0
        @test occursin("@", req["by"])
        @test abs(req["at"] - time()) < 60
        @test isfile(joinpath(control_dir(v), "requests", id * ".json"))
        @test isempty(read_acks(v, id))

        # Keys travel as JSON and come back the same keys.
        id2 = control!(v, :enqueue; keys=ks)
        back = [SweepRunner._key_from(d) for d in read_requests(v)[2]["keys"]]
        @test ParamIO.canonical.(back) == ParamIO.canonical.(ks)

        # By outdir: one request per (project, run) that has state.
        @test length(control!(outdir, :pause)) == 1
        @test isempty(control!(outdir, :pause; run="other"))
        @test isempty(control!(mktempdir(), :pause))
    end
end

@testset "cancel: matching queued keys are dropped, acknowledged, and logged with who asked" begin
    _ct_vault() do v, outdir
        ks = DataVault.keys(v)
        id = Ref("")
        work = _ct_once(() -> (id[] = control!(v, :cancel; select=Dict("N" => 8))))
        r = run!(work, v, ks; opts=_ct_opts())
        n8 = count(k -> k.params["N"] == 8, ks)
        @test n8 > 0
        @test r.cancelled == n8
        @test r.done == length(ks) - n8
        @test (r.err, r.busy, r.stop) == (0, 0, 0)
        @test all(k -> DataVault.is_done(v, k) == (k.params["N"] != 8), ks)

        ack = only(read_acks(v, id[]))
        @test ack["op"] == "cancel"
        @test ack["detail"]["cancelled"] == n8
        ev = only([e for e in _ct_events(outdir) if e.kind == "control_request"])
        @test ev.id == id[]
        @test ev.op == "cancel"
        @test occursin("@", ev.by)
        @test ev.detail.cancelled == n8
        @test only(read_status(v))["control"]["cancel_filters"] == 1

        # The cancel was for that job. The next one runs them.
        r2 = run!(_ct_ok, v, ks; opts=_ct_opts())
        @test r2.done == n8
        @test r2.cancelled == 0
    end
end

@testset "a request made before the master started is not for it" begin
    _ct_vault() do v, _
        ks = DataVault.keys(v)
        control!(v, :stop)
        control!(v, :cancel; select=Dict("N" => 8))
        sleep(0.05)
        r = run!(_ct_ok, v, ks; opts=_ct_opts())
        @test r.done == length(ks)
        @test (r.cancelled, r.stop) == (0, 0)
    end
end

@testset "a request for another master is not applied" begin
    _ct_vault() do v, _
        ks = DataVault.keys(v)
        work = _ct_once(() -> control!(v, :stop; master="somewhere_else_1"))
        r = run!(work, v, ks; opts=_ct_opts())
        @test r.done == length(ks)
    end
end

@testset "prioritise: matching keys go to the front of the queue" begin
    _ct_vault() do v, _
        ks = DataVault.keys(v)
        order = String[]
        target = ks[end]
        sel = Dict(String(n) => val for (n, val) in target.params)
        work = _ct_once(() -> control!(v, :prioritise; select=sel); record=order)
        r = run!(work, v, ks; opts=_ct_opts())
        @test r.done == length(ks)
        @test order[1] == ParamIO.canonical(ks[1])
        @test order[2] == ParamIO.canonical(target)          # was last, ran second
        @test sort(order) == sort(ParamIO.canonical.(ks))    # and nothing ran twice
    end
end

@testset "enqueue: keys are added to a running sweep, by key and by config" begin
    _ct_vault() do v, _
        ks = DataVault.keys(v)
        work = _ct_once(() -> control!(v, :enqueue; keys=ks[3:end]))
        r = run!(work, v, ks[1:2]; opts=_ct_opts())
        @test r.done == length(ks)
        @test r.total == length(ks)
        @test all(k -> DataVault.is_done(v, k), ks)
        @test all(k -> SweepRunner.is_complete(load_manifest(v), k), ks)
    end
    _ct_vault() do v, _
        ks = DataVault.keys(v)
        id = Ref("")
        work = _ct_once(() -> (id[] = control!(v, :enqueue; config=_CT_CFG)))
        r = run!(work, v, ks[1:1]; opts=_ct_opts())
        @test r.done == length(ks)
        d = only(read_acks(v, id[]))["detail"]
        @test d["keys"] == length(ks)
        @test d["queued"] == length(ks) - 1          # the one already in the table is not added
    end
end

@testset "enqueue: a key that is already done is settled, not recomputed" begin
    _ct_vault() do v, _
        ks = DataVault.keys(v)
        DataVault.save!(v, ks[end], Dict{String,Any}("x" => 0))
        DataVault.mark_done!(v, ks[end])
        ran = String[]
        work = _ct_once(() -> control!(v, :enqueue; keys=ks[2:end]); record=ran)
        r = run!(work, v, ks[1:1]; opts=_ct_opts())
        @test r.done == length(ks) - 1
        @test !(ParamIO.canonical(ks[end]) in ran)
    end
end

@testset "pause / resume: nothing is handed out while paused, and the queue is kept" begin
    _ct_vault() do v, outdir
        ks = DataVault.keys(v)
        stamps = Float64[]
        work = _ct_once() do
            control!(v, :pause)
            @async (sleep(1.5); control!(v, :resume))
        end
        timed = k -> (push!(stamps, time()); work(k))
        r = run!(timed, v, ks; opts=_ct_opts())
        @test r.done == length(ks)
        @test stamps[2] - stamps[1] >= 1.4               # the second key waited for the resume
        ops = [e.op for e in _ct_events(outdir) if e.kind == "control_request"]
        @test ops == ["pause", "resume"]
    end
end

@testset "stop with no scope: queued keys are attributed to the request and the loop ends" begin
    _ct_vault() do v, _
        ks = DataVault.keys(v)
        work = _ct_once(() -> control!(v, :stop))
        r = run!(work, v, ks; opts=_ct_opts())
        @test r.done == 1
        @test r.stop == length(ks) - 1
        @test r.stopped_by === :request
        @test (r.err, r.busy) == (0, 0)
    end
    _ct_vault() do v, _
        ks = DataVault.keys(v)
        work = _ct_once(() -> control!(v, :stop))
        t0 = time()
        r = run_loop!(work, v, ks; opts=_ct_opts(), idle_sleep=30.0)
        @test r.stopped_by === :request
        @test r.done == 1
        @test time() - t0 < 25                           # it did not sit out idle rounds
    end
end

@testset "should_stop: a unit told to stop leaves at its safe point, at no attempt" begin
    @test should_stop() == false                         # outside a run!
    @test stop_point() === nothing
    _ct_vault() do v, outdir
        ks = DataVault.keys(v)
        saw = Bool[]
        work =
            k -> begin
                push!(saw, should_stop(; poll=0))
                control!(v, :stop; select=Dict("N" => k.params["N"], "J" => k.params["J"]))
                for _ in 1:200
                    should_stop(; poll=0) && break
                    sleep(0.01)
                end
                stop_point(; poll=0)
                return _ct_ok(k)
            end
        r = run!(work, v, ks[1:1]; opts=_ct_opts(; max_attempts=3))
        @test saw == [false]                             # one attempt: a stop is not retried
        @test (r.done, r.err, r.stop) == (0, 0, 1)
        @test r.stopped_by === :request
        @test !DataVault.is_done(v, ks[1])
        @test !DataVault.is_running(v, ks[1])            # the lock was released
        kinds = [e.kind for e in _ct_events(outdir)]
        @test "key_stopped" in kinds
        @test !("error" in kinds)
    end
end

@testset "should_stop: the job's own flag and deadline are seen inside a key too" begin
    _ct_vault() do v, outdir
        ks = DataVault.keys(v)
        flag = joinpath(outdir, "STOP")
        work = k -> begin
            touch(flag)
            stop_point(; poll=0)
            return _ct_ok(k)
        end
        r = run!(work, v, ks; opts=_ct_opts(; stop_flag=flag))
        @test r.done == 0
        @test r.stop == length(ks)
        @test r.stopped_by === :flag                     # the job's bound outranks "a request"
    end
end

@testset "workers: a unit that outlives its grace is cut and its lock released" begin
    _ct_workers(2) do
        _ct_vault() do v, outdir
            ks = DataVault.keys(v)
            first_key = ks[1]
            sel = Dict(String(n) => val for (n, val) in first_key.params)
            # Never looks at should_stop: only the cut can free the key before it finishes.
            work = k -> (sleep(5.0); Dict{String,Any}("x" => 1))
            t = @async run!(
                work, v, ks; opts=RunOpts(; control_interval=0.2, status_interval=0.2)
            )
            t0 = time()
            while !DataVault.is_running(v, first_key) && time() - t0 < 60
                sleep(0.05)
            end
            @test DataVault.is_running(v, first_key)
            control!(v, :stop; select=sel, grace=0.3)
            freed_at = nothing
            while !istaskdone(t) && time() - t0 < 120
                if !DataVault.is_running(v, first_key)
                    freed_at = time()
                    break
                end
                sleep(0.05)
            end
            @test freed_at !== nothing
            @test !istaskdone(t)                          # freed while the unit was still running
            r = fetch(t)
            @test !DataVault.is_done(v, first_key)        # its late result was refused
            @test r.stop == 1
            @test r.done == length(ks) - 1
            @test r.err == 0
            cut = only([e for e in _ct_events(outdir) if e.kind == "key_cut"])
            @test cut.key == ParamIO.canonical(first_key)
        end
    end
end

@testset "workers: resize retires workers, and starts more through the spawn hook" begin
    _ct_workers(3) do
        _ct_vault(; cfg=_CT_BIG) do v, outdir
            ks = DataVault.keys(v)
            work = k -> (sleep(0.2); Dict{String,Any}("x" => 1))
            t = @async run!(work, v, ks; opts=RunOpts(; control_interval=0.2))
            sleep(0.6)
            id = control!(v, :resize; n=1)
            r = fetch(t)
            @test r.done == length(ks)
            @test only(read_acks(v, id))["detail"]["retiring"] == 2
            retired = [e for e in _ct_events(outdir) if e.kind == "worker_retired"]
            @test length(retired) == 2
            t0 = time()
            while nworkers() > 1 && time() - t0 < 60
                sleep(0.1)
            end
            @test nworkers() == 1
        end
    end
    _ct_workers(1) do
        _ct_vault(; cfg=_CT_BIG) do v, outdir
            ks = DataVault.keys(v)
            first_pid = only(workers())
            proj = dirname(Base.active_project())
            spawn = n -> addprocs(n; exeflags="--project=$proj")
            work = k -> (sleep(1.5); Dict{String,Any}("pid" => Distributed.myid()))
            # `load` is what readies a worker that joins mid-round, as it does the first ones.
            t = @async run!(
                work,
                v,
                ks;
                opts=RunOpts(; control_interval=0.2),
                spawn=spawn,
                load=:Distributed,
            )
            sleep(0.5)
            id = control!(v, :resize; n=2)
            r = fetch(t)
            @test r.done == length(ks)
            @test only(read_acks(v, id))["detail"]["spawning"] == 1
            @test nworkers() == 2
            joined = only([e for e in _ct_events(outdir) if e.kind == "workers_joined"])
            @test joined.n == 1
            # The worker that joined mid-round did part of the work.
            pids = Set(DataVault.load(v, k)["pid"] for k in ks)
            @test length(pids) == 2 && first_pid in pids
        end
    end
end

@testset "workers: without a spawn hook, growing is refused and said so" begin
    _ct_workers(1) do
        _ct_vault() do v, _
            ks = DataVault.keys(v)
            work = k -> (sleep(0.5); Dict{String,Any}("x" => 1))
            t = @async run!(work, v, ks; opts=RunOpts(; control_interval=0.2))
            sleep(0.4)
            id = control!(v, :resize; n=4)
            r = fetch(t)
            @test r.done == length(ks)
            @test occursin("spawn", only(read_acks(v, id))["detail"]["unsupported"])
        end
    end
end

@testset "workers: a drained node is not dispatched to" begin
    _ct_workers(2) do
        _ct_vault(; cfg=_CT_BIG) do v, _
            ks = DataVault.keys(v)
            work = k -> (sleep(0.3); Dict{String,Any}("x" => 1))
            t = @async run!(work, v, ks; opts=RunOpts(; control_interval=0.2))
            # Once a key has finished, not after a fixed wait: how long the first key takes to
            # start depends on the machine.
            t0 = time()
            while !any(k -> DataVault.is_done(v, k), ks) && time() - t0 < 120
                sleep(0.05)
            end
            control!(v, :drain; node=gethostname())       # every worker is on this node
            r = fetch(t)
            @test 0 < r.done < length(ks)
            @test r.busy == length(ks) - r.done           # left for another job, not failed
            @test r.err == 0
            @test gethostname() in only(read_status(v))["control"]["drained"]
        end
    end
end

@testset "a unit sees a stop for its node, and a cancel that covers running units" begin
    _ct_vault() do v, _
        ks = DataVault.keys(v)
        # A stop scoped to this node: the unit on it leaves, and the node is drained.
        work = k -> begin
            control!(v, :stop; node=gethostname())
            stop_point(; poll=0)
            return _ct_ok(k)
        end
        r = run!(work, v, ks[1:1]; opts=_ct_opts())
        @test (r.done, r.stop) == (0, 1)
        @test gethostname() in only(read_status(v))["control"]["drained"]
        # A stop for some other node is not for this unit.
        r = run!(
            k -> (control!(v, :stop; node="elsewhere"); stop_point(; poll=0); _ct_ok(k)),
            v,
            ks[1:1];
            opts=_ct_opts(),
        )
        @test r.done == 1
    end
    _ct_vault() do v, _
        ks = DataVault.keys(v)
        sel = Dict(String(n) => val for (n, val) in ks[1].params)
        # cancel with running=true reaches the unit that is running; without it, it does not.
        r = run!(
            k -> (control!(v, :cancel; select=sel); stop_point(; poll=0); _ct_ok(k)),
            v,
            ks[1:1];
            opts=_ct_opts(),
        )
        @test r.done == 1
    end
    _ct_vault() do v, _
        ks = DataVault.keys(v)
        sel = Dict(String(n) => val for (n, val) in ks[1].params)
        work = k -> begin
            control!(v, :cancel; select=sel, running=true)
            stop_point(; poll=0)
            return _ct_ok(k)
        end
        r = run!(work, v, ks[1:1]; opts=_ct_opts())
        @test (r.done, r.stop) == (0, 1)
    end
end

@testset "a request that cannot be applied is acknowledged with why, and the run goes on" begin
    _ct_vault() do v, outdir
        ks = DataVault.keys(v)
        bad = joinpath(outdir, "not_a_config.toml")
        write(bad, "this is = not [toml")
        ids = String[]
        work = _ct_once() do
            push!(ids, control!(v, :enqueue; config=bad))
            push!(ids, control!(v, :resize; n=8))           # there is no worker pool to resize
            push!(ids, control!(v, :drain; node="c099"))
            return nothing
        end
        r = run!(work, v, ks; opts=_ct_opts())
        @test r.done == length(ks)
        @test haskey(only(read_acks(v, ids[1]))["detail"], "error")
        @test only(read_acks(v, ids[2]))["detail"]["unsupported"] == "no worker pool"
        @test isempty(only(read_acks(v, ids[3]))["detail"])
        st = only(read_status(v))
        @test st["control"]["drained"] == ["c099"]
        @test st["control"]["target_workers"] == 8
        text = sprint(io -> print_status(io, v))
        @test occursin("control  ", text) && occursin("drained: c099", text)
    end
end

@testset "workers: a spawn hook that fails is logged, and the round is not disturbed" begin
    _ct_workers(1) do
        _ct_vault() do v, outdir
            ks = DataVault.keys(v)
            work = k -> (sleep(0.5); Dict{String,Any}("x" => 1))
            t = @async run!(
                work,
                v,
                ks;
                opts=RunOpts(; control_interval=0.2),
                spawn=n -> error("no more nodes"),
            )
            sleep(0.4)
            control!(v, :resize; n=3)
            r = fetch(t)
            @test r.done == length(ks)
            failed = only([e for e in _ct_events(outdir) if e.kind == "spawn_failed"])
            @test failed.n == 2
            @test occursin("no more nodes", failed.err)
        end
    end
end

@testset "cli: requests from a shell" begin
    _ct_vault() do v, outdir
        io = IOBuffer()
        # No sweep has run here yet: there is nobody to tell.
        @test SweepRunner.cli(["pause", outdir]; io=io) == 1
        run!(_ct_ok, v, DataVault.keys(v))
        @test SweepRunner.cli(["pause", outdir]; io=io) == 0
        @test SweepRunner.cli(
            [
                "cancel",
                outdir,
                "--select",
                "N=4,8",
                "--select",
                "J=0.5",
                "--samples",
                "1,2",
            ];
            io=io,
        ) == 0
        @test SweepRunner.cli(["stop", outdir, "--node", "c01", "--grace", "90"]; io=io) ==
            0
        @test SweepRunner.cli(["resize", outdir, "--n", "12", "--run", "ct"]; io=io) == 0
        reqs = read_requests(v)
        @test [r["op"] for r in reqs] == ["pause", "cancel", "stop", "resize"]
        @test reqs[2]["select"] == Dict("N" => [4, 8], "J" => [0.5])
        @test reqs[2]["samples"] == [1, 2]
        @test (reqs[3]["node"], reqs[3]["grace"]) == ("c01", 90.0)
        @test reqs[4]["n"] == 12
        # Usage errors.
        @test SweepRunner.cli(["resize", outdir]; io=io) == 2
        @test SweepRunner.cli(["cancel", outdir, "--select", "N"]; io=io) == 2
        @test SweepRunner.cli(["stop", outdir, "--bogus"]; io=io) == 2
        @test SweepRunner.cli(["stop"]; io=io) == 2
        @test SweepRunner.cli(["resize", outdir, "--n", "3", "--run", "nope"]; io=io) == 1
    end
end
