# run! and the task table (#64): the master reads the markers once, hands a key out with its lock
# token and resume point, and takes a dead worker's lock back itself.

using SweepRunner, Test, DataVault, ParamIO, JSON3, Distributed
using SweepRunner: progress_dir

const _TT_CFG = joinpath(@__DIR__, "fixtures", "study.toml")

function _tt_vault(f; run="tt")
    outdir = mktempdir()
    try
        f(DataVault.Vault(_TT_CFG; run=run, outdir=outdir), outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

function _tt_events(outdir)
    logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir(outdir))
    return [JSON3.read(l) for f in logs for l in readlines(joinpath(outdir, f))]
end

function _tt_workers(f, n)
    nprocs() > 1 && rmprocs(workers())
    addprocs(n; exeflags="--project=$(dirname(Base.active_project()))")
    try
        @everywhere workers() Core.eval(
            Main, :(using SweepRunner, DataVault, ParamIO, Distributed)
        )
        f()
    finally
        rmprocs(workers())
    end
end

@testset "progress: outside a run! there is nothing to resume and nothing is written" begin
    @test resume_point() === nothing
    @test report_progress(3) == false
end

@testset "progress: a retry resumes from what the failed attempt reported" begin
    _tt_vault() do v, _
        k = DataVault.keys(v)[1]
        seen = Any[]
        work = key -> begin
            p = resume_point()
            push!(seen, p === nothing ? nothing : (p.step, p.of))
            p === nothing || return Dict{String,Any}("from" => p.step)
            @test report_progress(2; of=4) == true
            error("cut after step 2")
        end
        r = run!(work, v, [k]; opts=RunOpts(; max_attempts=2))
        @test r.done == 1
        @test seen == [nothing, (2, 4)]
        # A finished unit has no resume point left behind.
        @test isempty(read_progress(v))
    end
end

@testset "progress: the next run! is handed the step a failed run reached" begin
    _tt_vault() do v, _
        k = DataVault.keys(v)[1]
        fail = key -> (report_progress(5; of=16, note="seg"); error("wall clock"))
        r = run!(fail, v, [k]; opts=RunOpts(; max_attempts=1))
        @test (r.done, r.err) == (0, 1)
        p = read_progress(v)[ParamIO.canonical(k)]
        @test (p.step, p.of, p.note) == (5, 16, "seg")
        @test isfile(joinpath(progress_dir(v), only(readdir(progress_dir(v)))))

        got = Ref{Any}(nothing)
        r = run!(key -> (got[]=resume_point(); Dict{String,Any}("x" => 1)), v, [k])
        @test r.done == 1
        @test got[].step == 5
        @test isempty(read_progress(v))
    end
end

@testset "scan: a key held by a live sibling is not handed out, and is counted busy" begin
    _tt_vault() do v, outdir
        ks = DataVault.keys(v)
        held = ks[2]
        sib = owner_token()                                   # this process: provably alive
        @test DataVault.acquire_running!(v, held, sib) === :ok
        ran = String[]
        r = run!(
            k -> (push!(ran, ParamIO.canonical(k)); Dict{String,Any}("x" => 1)),
            v,
            ks;
            opts=RunOpts(; log_level=:debug),
        )
        @test r.done == length(ks) - 1
        @test r.busy == 1
        @test !(ParamIO.canonical(held) in ran)
        @test DataVault.running_owner(v, held) == sib         # the sibling's lock is untouched
        ev = _tt_events(outdir)
        # Found by the master's scan: no acquisition was attempted for it.
        @test count(e -> e.kind == "lock_busy", ev) >= 1
        @test !any(e -> e.kind == "key_acquired" && e.key == ParamIO.canonical(held), ev)
    end
end

@testset "scan: a key a sibling finished since the manifest is settled, not recomputed" begin
    _tt_vault() do v, _
        ks = DataVault.keys(v)
        DataVault.save!(v, ks[1], Dict{String,Any}("x" => 0))
        DataVault.mark_done!(v, ks[1])
        ran = Ref(0)
        r = run!(k -> (ran[] += 1; Dict{String,Any}("x" => 1)), v, ks)
        @test ran[] == length(ks) - 1
        @test r.done == length(ks) - 1
        @test SweepRunner.is_complete(load_manifest(v), ks[1])
    end
end

@testset "drain: a key released while the pass ran is finished in the same run!" begin
    _tt_vault() do v, _
        ks = DataVault.keys(v)
        sib = owner_token()
        @test DataVault.acquire_running!(v, ks[1], sib) === :ok
        # The sibling lets go while this master is on its last key.
        last_k = ParamIO.canonical(ks[end])
        work =
            k -> begin
                ParamIO.canonical(k) == last_k && DataVault.clear_running!(v, ks[1], sib)
                return Dict{String,Any}("x" => 1)
            end
        r = run!(work, v, ks)
        @test r.done == length(ks)
        @test r.busy == 0
        @test all(k -> DataVault.is_done(v, k), ks)
    end
end

@testset "workers: every key is handed out exactly once and named by the master" begin
    _tt_workers(3) do
        _tt_vault() do v, outdir
            ks = DataVault.keys(v)
            r = run!(k -> Dict{String,Any}("pid" => Distributed.myid()), v, ks)
            @test r.done == length(ks)
            ev = _tt_events(outdir)
            acquired = [String(e.key) for e in ev if e.kind == "key_acquired"]
            @test sort(acquired) == sort(ParamIO.canonical.(ks))
            @test all(k -> !DataVault.is_running(v, k), ks)
        end
    end
end

@testset "workers: a dead worker's lock is released at once and its progress handed on" begin
    # `stale_after` is ten minutes: the only way the key completes inside this test is the master
    # taking back the lock it named.
    _tt_workers(2) do
        _tt_vault() do v, outdir
            ks = DataVault.keys(v)
            poison = ParamIO.canonical(ks[1])
            died = joinpath(outdir, "died")
            from = joinpath(outdir, "resumed_from")
            work = k -> begin
                if ParamIO.canonical(k) == poison
                    if !isfile(died)
                        SweepRunner.report_progress(3; of=8)
                        touch(died)
                        ccall(:_exit, Cvoid, (Cint,), 1)
                    end
                    p = SweepRunner.resume_point()
                    write(from, string(p === nothing ? -1 : p.step))
                end
                return Dict{String,Any}("x" => 1)
            end
            t0 = time()
            r = run!(
                work, v, ks; opts=RunOpts(; stale_after=600.0, heartbeat_interval=60.0)
            )
            @test time() - t0 < 120
            @test r.done == length(ks)
            @test read(from, String) == "3"
            ev = _tt_events(outdir)
            rel = [e for e in ev if e.kind == "lock_released"]
            @test length(rel) == 1
            @test rel[1].key == poison
            @test rel[1].why == "worker_exited"
            @test isempty(read_progress(v))
        end
    end
end
