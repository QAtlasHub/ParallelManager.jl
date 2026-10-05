# Failures in the bookkeeping are said (#108): an unreadable checkpoint or progress stamp, errors
# in run_loop!'s result, a status that cannot be written.

using SweepRunner, Test, DataVault, ParamIO, JSON3
using SweepRunner: checkpoint, checkpoint_dir, progress_dir

const _FL_CFG = joinpath(@__DIR__, "fixtures", "study.toml")

function _fl_vault(f; run="fl")
    outdir = mktempdir()
    try
        f(DataVault.Vault(_FL_CFG; run=run, outdir=outdir), outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

function _fl_events(outdir)
    logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir(outdir))
    return [JSON3.read(l) for f in logs for l in readlines(joinpath(outdir, f))]
end

_fl_quiet(; kw...) = RunOpts(; control_interval=0, kw...)

@testset "an unreadable checkpoint is kept aside and reported, not overwritten in silence" begin
    _fl_vault() do v, outdir
        k = DataVault.keys(v)[1]
        mkpath(checkpoint_dir(v))
        name = SweepRunner._key_hash(ParamIO.canonical(k)) * ".jld2"
        write(joinpath(checkpoint_dir(v), name), "what a truncated write left")
        got = Ref{Any}(:unset)
        work = key -> begin
            cp = checkpoint()
            got[] = load_checkpoint(cp)
            save_checkpoint!(cp, 1; step=1)                 # would have replaced the bad file
            error("stop here, so the checkpoint is not cleaned up")
        end
        run!(work, v, [k]; opts=_fl_quiet(; max_attempts=1))
        @test got[] === nothing                             # it starts over
        files = readdir(checkpoint_dir(v))
        aside = only(filter(f -> occursin(".unreadable.", f), files))
        @test read(joinpath(checkpoint_dir(v), aside), String) ==
            "what a truncated write left"
        ev = only([e for e in _fl_events(outdir) if e.kind == "checkpoint_unreadable"])
        @test ev.key == ParamIO.canonical(k)
        @test endswith(ev.kept, aside)
        @test !isempty(ev.err)
    end
end

@testset "progress stamps that cannot be read are counted and said" begin
    _fl_vault() do v, outdir
        ks = DataVault.keys(v)
        mkpath(progress_dir(v))
        write(joinpath(progress_dir(v), "aaaa.json"), "{ not json")
        write(joinpath(progress_dir(v), "bbbb.json"), "")
        n = Ref(0)
        @test isempty(read_progress(v; unreadable=n))
        @test n[] == 2
        run!(k -> Dict{String,Any}("x" => 1), v, ks; opts=_fl_quiet())
        ev = only([e for e in _fl_events(outdir) if e.kind == "progress_unreadable"])
        @test ev.files == 2
    end
end

@testset "run_loop!: a stage whose keys all fail does not look like a clean finish" begin
    _fl_vault() do v, _
        ks = DataVault.keys(v)
        r = run_loop!(
            k -> error("always"),
            v,
            ks;
            opts=_fl_quiet(; max_attempts=2),
            max_empty_rounds=1,
            idle_sleep=0.01,
        )
        @test (r.done, r.busy) == (0, 0)                    # what a clean finish also returns
        @test r.err == length(ks)
        @test r.gave_up == length(ks)
        @test r.remaining == length(ks)
        @test r.stopped_by === nothing
        ok = run_loop!(k -> Dict{String,Any}("x" => 1), v, ks; opts=_fl_quiet())
        @test (ok.done, ok.err, ok.gave_up, ok.remaining) == (length(ks), 0, 0, 0)
    end
end

@testset "a status that cannot be written is said once, and the run goes on" begin
    _fl_vault() do v, outdir
        ks = DataVault.keys(v)
        # A file where the masters directory has to be: every status write fails.
        mkpath(state_root(v))
        write(joinpath(state_root(v), "masters"), "in the way")
        r = run!(
            k -> (sleep(0.05); Dict{String,Any}("x" => 1)),
            v,
            ks;
            opts=_fl_quiet(; status_interval=0.01),
        )
        @test r.done == length(ks)
        @test count(e -> e.kind == "status_write_failed", _fl_events(outdir)) == 1
        @test isempty(read_status(v))
    end
end

@testset "a status file that cannot be read is skipped, with a warning" begin
    _fl_vault() do v, _
        run!(k -> Dict{String,Any}("x" => 1), v, DataVault.keys(v); opts=_fl_quiet())
        good = only(read_status(v))
        bad = joinpath(state_root(v), "masters", "elsewhere_1")
        mkpath(bad)
        write(joinpath(bad, "status.json"), "{ half a file")
        st = @test_logs (:warn, r"status file could not be read") match_mode = :any read_status(
            v
        )
        @test only(st)["master"] == good["master"]
    end
end
