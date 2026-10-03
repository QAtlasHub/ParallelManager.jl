# Checkpoint (#75): what a key keeps when it stops, as a service of the per-key pipeline.

using SweepRunner, Test, DataVault, ParamIO, JSON3, Distributed
using SweepRunner: checkpoint, checkpoint_dir

const _CK_CFG = joinpath(@__DIR__, "..", "run", "fixtures", "study.toml")

function _ck_vault(f; run="ck")
    outdir = mktempdir()
    try
        f(DataVault.Vault(_CK_CFG; run=run, outdir=outdir), outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

# A unit of `n` steps whose state is a running sum. `fail_at` makes it throw after that step, on
# the first attempt that reaches it without having resumed past it.
function _ck_work(n; fail_at=nothing, starts=Int[], every_step=true)
    return key -> begin
        cp = checkpoint()
        st = something(load_checkpoint(cp), (; step=0, acc=0.0))
        push!(starts, st.step)
        for s in (st.step + 1):n
            st = (; step=s, acc=st.acc + s * key.params["N"])
            if every_step || checkpoint_due(cp)
                save_checkpoint!(cp, st; step=s, of=n)
            end
            (fail_at == s && length(starts) == 1) && error("cut after step $s")
        end
        return Dict{String,Any}("acc" => st.acc, "steps" => st.step)
    end
end

@testset "outside a run! the handle is inert" begin
    cp = checkpoint()
    @test load_checkpoint(cp) === nothing
    @test save_checkpoint!(cp, 1) == false
    @test checkpoint_due(cp) == false
end

@testset "a retry resumes from the saved state, and a finished key leaves nothing behind" begin
    _ck_vault() do v, _
        k = DataVault.keys(v)[1]
        starts = Int[]
        r = run!(_ck_work(6; fail_at=4, starts), v, [k]; opts=RunOpts(; max_attempts=2))
        @test r.done == 1
        @test starts == [0, 4]                              # the second attempt began at step 4
        @test DataVault.load(v, k)["steps"] == 6
        @test DataVault.load(v, k)["acc"] == sum(1:6) * k.params["N"]
        @test !isdir(checkpoint_dir(v)) || isempty(readdir(checkpoint_dir(v)))
        @test isempty(read_progress(v))
    end
end

@testset "the next job resumes from it, with the progress stamp beside it" begin
    _ck_vault() do v, _
        k = DataVault.keys(v)[1]
        starts = Int[]
        r = run!(_ck_work(6; fail_at=3, starts), v, [k]; opts=RunOpts(; max_attempts=1))
        @test (r.done, r.err) == (0, 1)
        @test only(readdir(checkpoint_dir(v))) ==
            SweepRunner._key_hash(ParamIO.canonical(k)) * ".jld2"
        p = read_progress(v)[ParamIO.canonical(k)]
        @test (p.step, p.of) == (3, 6)

        starts2 = Int[]
        seen = Ref{Any}(nothing)
        work = key -> (seen[]=resume_point(); _ck_work(6; starts=starts2)(key))
        r = run!(work, v, [k])
        @test r.done == 1
        @test starts2 == [3]
        @test seen[].step == 3                              # the master handed the same point over
        @test DataVault.load(v, k)["acc"] == sum(1:6) * k.params["N"]
        @test isempty(readdir(checkpoint_dir(v)))
    end
end

@testset "a checkpoint that cannot be read is no checkpoint" begin
    _ck_vault() do v, _
        k = DataVault.keys(v)[1]
        mkpath(checkpoint_dir(v))
        write(
            joinpath(
                checkpoint_dir(v), SweepRunner._key_hash(ParamIO.canonical(k)) * ".jld2"
            ),
            "junk",
        )
        starts = Int[]
        r = run!(_ck_work(3; starts), v, [k])
        @test r.done == 1
        @test starts == [0]
    end
end

# The file a key's checkpoint lives in.
function _ck_file(v, k)
    return joinpath(
        checkpoint_dir(v), SweepRunner._key_hash(ParamIO.canonical(k)) * ".jld2"
    )
end

function _ck_events(outdir)
    logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir(outdir))
    return [JSON3.read(l) for f in logs for l in readlines(joinpath(outdir, f))]
end

@testset "damaged checkpoints: each kind starts over, is kept aside and said (#111)" begin
    # A real checkpoint to damage: cut a run after its second step.
    function real_checkpoint(v, k)
        run!(_ck_work(3; fail_at=2), v, [k]; opts=RunOpts(; max_attempts=1))
        @test isfile(_ck_file(v, k))
        return read(_ck_file(v, k))
    end
    damage = [
        "cut short, its header intact" =>
            (v, k, bytes) -> write(_ck_file(v, k), bytes[1:(length(bytes) ÷ 2)]),
        "a JLD2 file without the state" =>
            (v, k, bytes) -> SweepRunner.JLD2.jldsave(_ck_file(v, k); saved_at=time()),
        "empty" => (v, k, bytes) -> write(_ck_file(v, k), UInt8[]),
    ]
    for (what, break!) in damage
        _ck_vault() do v, outdir
            k = DataVault.keys(v)[1]
            bytes = real_checkpoint(v, k)
            break!(v, k, bytes)
            starts = Int[]
            r = run!(_ck_work(3; starts), v, [k])
            @testset "$what" begin
                @test r.done == 1
                @test starts == [0]                           # from the beginning
                @test DataVault.load(v, k)["acc"] == sum(1:3) * k.params["N"]
                ev = [e for e in _ck_events(outdir) if e.kind == "checkpoint_unreadable"]
                @test length(ev) == 1
                @test ev[1].kept !== nothing && isfile(ev[1].kept)   # not overwritten
            end
        end
    end
end

@testset "a temporary file left by a killed save is not a checkpoint (#111)" begin
    _ck_vault() do v, outdir
        k = DataVault.keys(v)[1]
        mkpath(checkpoint_dir(v))
        orphan = string(_ck_file(v, k)[1:(end - 5)], ".tmp.99999.12345.jld2")
        write(orphan, "half a save")
        starts = Int[]
        r = run!(_ck_work(3; starts), v, [k])
        @test r.done == 1
        @test starts == [0]
        @test !any(e -> e.kind == "checkpoint_unreadable", _ck_events(outdir))
        @test !isfile(_ck_file(v, k))                         # the key's own is cleaned up
    end
end

@testset "through run!, a resumed key gives what an uninterrupted one gives (#111)" begin
    _ck_vault() do v, _
        ks = DataVault.keys(v)
        k, plain = ks[1], ks[2]
        starts = Int[]
        # Fails after step 3; the retry resumes from the checkpoint of step 3.
        r = run!(_ck_work(6; fail_at=3, starts), v, [k]; opts=RunOpts(; max_attempts=2))
        @test r.done == 1
        @test starts == [0, 3]
        got = DataVault.load(v, k)
        run!(_ck_work(6), v, [plain])
        want = DataVault.load(v, plain)
        @test got["steps"] == want["steps"] == 6
        @test got["acc"] / k.params["N"] == want["acc"] / plain.params["N"] == sum(1:6)
    end
end

@testset "checkpoint_due: every checkpoint_every seconds" begin
    _ck_vault() do v, _
        k = DataVault.keys(v)[1]
        saved_at = Int[]
        work = key -> begin
            cp = checkpoint()
            for s in 1:12
                sleep(0.1)
                if checkpoint_due(cp)
                    save_checkpoint!(cp, s)
                    push!(saved_at, s)
                end
            end
            return Dict{String,Any}("x" => 1)
        end
        run!(work, v, [k]; opts=RunOpts(; checkpoint_every=0.35))
        # 1.2 s of work with a save every 0.35 s: three saves, not twelve and not none.
        @test 2 <= length(saved_at) <= 4
        @test all(d -> d >= 3, diff(saved_at))
    end
end

@testset "checkpoint_due: at once on a stop, and when the deadline is close" begin
    _ck_vault() do v, outdir
        k = DataVault.keys(v)[1]
        flag = joinpath(outdir, "STOP")
        due = Bool[]
        work =
            key -> begin
                cp = checkpoint()
                push!(due, checkpoint_due(cp))                 # nothing has happened yet
                touch(flag)
                sleep(0.05)
                push!(due, SweepRunner.should_stop(; poll=0) && checkpoint_due(cp))
                save_checkpoint!(cp, 1; step=1)
                SweepRunner.stop_point(; poll=0)
                return Dict{String,Any}("x" => 1)
            end
        r = run!(work, v, [k]; opts=RunOpts(; checkpoint_every=3600.0, stop_flag=flag))
        @test due == [false, true]
        @test (r.done, r.stop) == (0, 1)
        @test length(readdir(checkpoint_dir(v))) == 1      # what the next job starts from
    end
    _ck_vault() do v, _
        k = DataVault.keys(v)[1]
        due = Bool[]
        work = key -> begin
            cp = checkpoint()
            push!(due, checkpoint_due(cp))                 # the deadline is 30 s away: close
            save_checkpoint!(cp, 1)
            push!(due, checkpoint_due(cp))                 # saved since it came close: not again
            return Dict{String,Any}("x" => 1)
        end
        run!(work, v, [k]; opts=RunOpts(; checkpoint_every=3600.0, deadline=time() + 30))
        @test due == [true, false]
    end
end

@testset "check_checkpoints: the same result however often the key is cut" begin
    _ck_vault() do v, _
        k = DataVault.keys(v)[1]
        r = check_checkpoints(_ck_work(5), v, k)
        @test r.same
        @test r.restarts == 5                               # cut after each of the five saves
        @test r.plain["acc"] == sum(1:5) * k.params["N"]
        @test !isdir(checkpoint_dir(v)) || isempty(readdir(checkpoint_dir(v)))
        @test isempty(read_progress(v))

        # A work function whose result depends on where it was cut: it restarts its sum.
        broken = key -> begin
            cp = checkpoint()
            st = load_checkpoint(cp)
            from = st === nothing ? 1 : st + 1
            acc = 0                                         # forgotten state
            for s in from:4
                acc += s
                save_checkpoint!(cp, s; step=s)
            end
            return Dict{String,Any}("acc" => acc)
        end
        r = check_checkpoints(broken, v, k)
        @test !r.same
        @test r.plain["acc"] == 10

        # One that never advances from its checkpoint is reported, not looped on forever.
        stuck = key -> (save_checkpoint!(checkpoint(), 0); Dict{String,Any}("x" => 1))
        @test_throws ErrorException check_checkpoints(stuck, v, k; max_restarts=5)
    end
end

@testset "workers: a worker that dies resumes on another from the checkpoint" begin
    nprocs() > 1 && rmprocs(workers())
    addprocs(2; exeflags="--project=$(dirname(Base.active_project()))")
    try
        @everywhere workers() Core.eval(
            Main, :(using SweepRunner, DataVault, ParamIO, Distributed)
        )
        _ck_vault() do v, outdir
            k = DataVault.keys(v)[1]
            died = joinpath(outdir, "died")
            from = joinpath(outdir, "from")
            work = key -> begin
                cp = SweepRunner.checkpoint()
                st = something(SweepRunner.load_checkpoint(cp), 0)
                write(from, string(st))
                for s in (st + 1):4
                    SweepRunner.save_checkpoint!(cp, s; step=s, of=4)
                    if s == 2 && !isfile(died)
                        touch(died)
                        ccall(:_exit, Cvoid, (Cint,), 1)
                    end
                end
                return Dict{String,Any}("steps" => 4)
            end
            r = run!(work, v, [k])
            @test r.done == 1
            @test read(from, String) == "2"
            @test isempty(readdir(checkpoint_dir(v)))
        end
    finally
        rmprocs(workers())
        note_workers!(; planned=0, launched=0)
    end
end
