# Start-up and the master's load (#71, #72, #76, #78): the limit per master is a message, the
# manifest is kept while the round runs, the round says where its time went, workers keep their
# own logs, and a round's context travels to a worker once.

using SweepRunner, Test, DataVault, ParamIO, JSON3, Distributed

const _SU_CFG = joinpath(@__DIR__, "fixtures", "study.toml")

function _su_vault(f; run="su")
    outdir = mktempdir()
    try
        f(DataVault.Vault(_SU_CFG; run=run, outdir=outdir), outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

function _su_events(outdir)
    logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir(outdir))
    return [JSON3.read(l) for f in logs for l in readlines(joinpath(outdir, f))]
end

function _su_workers(f, n)
    nprocs() > 1 && rmprocs(workers())
    addprocs(n; exeflags="--project=$(dirname(Base.active_project()))")
    try
        @everywhere workers() Core.eval(
            Main, :(using SweepRunner, DataVault, ParamIO, Distributed)
        )
        f()
    finally
        rmprocs(workers())
        SweepRunner._WORKER_LOG_DIR[] = nothing
        note_workers!(; planned=0, launched=0)
    end
end

@testset "srun_worker_limit: read from the cluster's port range" begin
    cfg = """
    SlurmctldPort           = 6817
    SrunPortRange           = 52501-65000
    SuspendTime             = INFINITE
    """
    @test srun_worker_limit(cfg) == 1607               # 12500 ports / 7, a tenth held back
    @test srun_worker_limit("SrunPortRange = 60001-60070") == 9
    @test srun_worker_limit("SlurmctldPort = 6817\n") === nothing
    @test srun_worker_limit("SrunPortRange = (null)\n") === nothing
    @test srun_worker_limit("SrunPortRange = 100-50\n") === nothing
end

@testset "a master asked for more workers than it can start says so" begin
    check = SweepRunner._check_worker_limit
    @test check(100, nothing) === nothing
    @test check(1607, 1607) === nothing
    err = try
        check(3735, 1607)
    catch e
        e
    end
    @test err isa ArgumentError
    @test occursin("3735 workers", err.msg)
    @test occursin("Run 3 masters", err.msg)           # how many it would take
    @test occursin("SWEEPRUNNER_SHARD", err.msg)
    withenv("SWEEPRUNNER_MAX_WORKERS" => "250", "SLURM_JOB_ID" => nothing) do
        @test SweepRunner._max_workers_default() == 250
    end
    withenv("SWEEPRUNNER_MAX_WORKERS" => nothing, "SLURM_JOB_ID" => nothing) do
        @test SweepRunner._max_workers_default() === nothing     # outside a job: no opinion
    end
end

@testset "split_nodes: one group per master" begin
    @test split_nodes(["a", "b", "c", "d", "e"], 2) == [["a", "b", "c"], ["d", "e"]]
    @test split_nodes("c[001-006]", 3) ==
        [["c001", "c002"], ["c003", "c004"], ["c005", "c006"]]
    @test split_nodes(["a", "b"], 5) == [["a"], ["b"]]             # no empty groups
    @test split_nodes(["a"], 1) == [["a"]]
    @test_throws ArgumentError split_nodes(["a"], 0)
    # Every node is in exactly one group.
    g = split_nodes("n[01-72]", 5)
    @test length(g) == 5 && sum(length, g) == 72
    @test maximum(length, g) - minimum(length, g) <= 1
end

@testset "sysimage: the flag workers are started with" begin
    @test SweepRunner._worker_exeflags(nothing) == String[]
    @test SweepRunner._worker_exeflags("") == String[]
    img = joinpath(mktempdir(), "app.so")
    touch(img)
    @test SweepRunner._worker_exeflags(img) == ["--sysimage=$img"]
    @test_throws ArgumentError SweepRunner._worker_exeflags("/no/such/image.so")
end

@testset "todo_count: is there anything to do, before starting a worker" begin
    _su_vault() do v, _
        ks = DataVault.keys(v)
        @test todo_count(v, ks) == length(ks)
        run!(k -> Dict{String,Any}("x" => 1), v, ks[1:2])
        @test todo_count(v, ks) == length(ks) - 2
        # A key finished by someone who has not written the manifest yet is seen too.
        DataVault.save!(v, ks[3], Dict{String,Any}("x" => 0))
        DataVault.mark_done!(v, ks[3])
        @test todo_count(v, ks) == length(ks) - 3
    end
end

@testset "the manifest is kept while the round runs" begin
    _su_vault() do v, _
        ks = DataVault.keys(v)
        seen = Int[]
        work = k -> begin
            push!(seen, length(load_manifest(v).complete))
            sleep(0.05)
            return Dict{String,Any}("x" => 1)
        end
        run!(work, v, ks; opts=RunOpts(; manifest_interval=0.01))
        # Each key found the ones before it already in the manifest: a job killed here would
        # leave them there.
        @test seen == collect(0:(length(ks) - 1))
    end
    _su_vault() do v, _
        ks = DataVault.keys(v)
        seen = Int[]
        work =
            k ->
                (push!(seen, length(load_manifest(v).complete)); Dict{String,Any}("x" => 1))
        run!(work, v, ks; opts=RunOpts(; manifest_interval=0))
        @test all(==(0), seen)                         # only at the end, as before
        @test length(load_manifest(v).complete) == length(ks)
    end
end

@testset "stage_done says where the round's time went" begin
    _su_vault() do v, outdir
        ks = DataVault.keys(v)
        run!(k -> (sleep(0.1); Dict{String,Any}("x" => 1)), v, ks)
        e = only([e for e in _su_events(outdir) if e.kind == "stage_done"])
        for f in (:prepare_secs, :scan_secs, :manifest_secs, :dispatch_secs, :total_secs)
            @test haskey(e, f) && e[f] >= 0
        end
        @test e.dispatch_secs >= 0.1 * length(ks)
        @test e.total_secs >= e.dispatch_secs
    end
end

@testset "workers: each keeps its own log, and the round's context is sent once" begin
    _su_workers(2) do
        _su_vault() do v, outdir
            ks = DataVault.keys(v)
            logs = joinpath(outdir, "logs")
            files = worker_logs!(logs)
            @test sort(collect(keys(files))) == sort(workers())
            @test all(isfile, values(files))
            work =
                k -> begin
                    println("hello from ", Distributed.myid(), " on ", ParamIO.canonical(k))
                    flush(stdout)
                    return Dict{String,Any}("x" => 1)
                end
            r = run!(work, v, ks)
            @test r.done == length(ks)
            text = join(read(f, String) for f in values(files))
            for k in ks
                @test occursin("on $(ParamIO.canonical(k))", text)
            end
            # A worker's file holds only that worker's lines.
            for (pid, f) in files
                other = Regex("hello from (?!$(pid)\\b)\\d+")
                @test !occursin(other, read(f, String))
            end
            # The round was installed on each worker and dropped when it ended.
            left = [
                remotecall_fetch(() -> length(SweepRunner._ROUNDS), p) for p in workers()
            ]
            t0 = time()
            while any(>(0), left) && time() - t0 < 30
                sleep(0.1)
                left = [
                    remotecall_fetch(() -> length(SweepRunner._ROUNDS), p) for
                    p in workers()
                ]
            end
            @test all(==(0), left)
        end
    end
end

@testset "workers: the per-tick work runs — the manifest is kept, and nothing fails quietly (#98)" begin
    _su_workers(2) do
        _su_vault() do v, outdir
            ks = DataVault.keys(v)
            seen = joinpath(outdir, "seen")
            mkpath(seen)
            # The last key waits until the manifest holds the keys before it: only the ticker can
            # put them there while the round is still running.
            lastk = ParamIO.canonical(ks[end])
            work = k -> begin
                if ParamIO.canonical(k) == lastk
                    t0 = time()
                    n = 0
                    while time() - t0 < 30
                        n = length(SweepRunner.load_manifest(v).complete)
                        n >= 1 && break
                        sleep(0.1)
                    end
                    write(joinpath(seen, "n"), string(n))
                end
                return Dict{String,Any}("x" => 1)
            end
            r = run!(
                work, v, ks; opts=RunOpts(; manifest_interval=0.1, control_interval=0.2)
            )
            @test r.done == length(ks)
            @test parse(Int, read(joinpath(seen, "n"), String)) >= 1
            kinds = [e.kind for e in _su_events(outdir)]
            @test !("control_failed" in kinds)
            @test !("manifest_failed" in kinds)
        end
    end
end

@testset "workers: a key that cannot fit before the deadline is not handed to one (#111)" begin
    _su_workers(2) do
        _su_vault() do v, outdir
            ks = DataVault.keys(v)
            long = Set(ParamIO.canonical.(ks[1:2]))
            need = k -> ParamIO.canonical(k) in long ? 1000.0 : 0.01
            r = run!(
                k -> Dict{String,Any}("pid" => Distributed.myid()),
                v,
                ks;
                opts=RunOpts(; deadline_in=60, control_interval=0),
                min_time=need,
            )
            @test r.held_back == 2
            @test r.done == length(ks) - 2
            @test (r.busy, r.err, r.stop) == (0, 0, 0)
            @test r.stopped_by === nothing
            @test all(k -> DataVault.is_done(v, k) == !(ParamIO.canonical(k) in long), ks)
            # The ones that ran, ran on the workers.
            ran = [k for k in ks if !(ParamIO.canonical(k) in long)]
            @test all(k -> DataVault.load(v, k)["pid"] != 1, ran)
            ev = only([e for e in _su_events(outdir) if e.kind == "held_back"])
            @test ev.keys == 2
        end
    end
end

@testset "a key of a round that is not installed is an error, not a silent nothing" begin
    k = ParamIO.DataKey(Dict{String,Any}("N" => 1), 1)
    @test_throws ErrorException SweepRunner._run_installed(UInt64(42), k)
end
