using ParallelManager, Test, DataVault, ParamIO, JSON3

isdefined(@__MODULE__, :FIXTURE_CFG) ||
    (const FIXTURE_CFG = joinpath(@__DIR__, "fixtures", "study.toml"))

_kinds(path) = [JSON3.read(l).kind for l in readlines(path) if !isempty(l)]

@testset "EventLog: min_level filters by event level" begin
    mktempdir() do dir
        p = joinpath(dir, "e.jsonl")
        log = ParallelManager.EventLog(p)                 # default :info
        ParallelManager.log_event(log, :noisy; level=:debug)
        ParallelManager.log_event(log, :important)        # level defaults to :info
        k = _kinds(p)
        @test !("noisy" in k)                             # :debug dropped
        @test "important" in k

        p2 = joinpath(dir, "e2.jsonl")
        log2 = ParallelManager.EventLog(p2; min_level=:debug)
        ParallelManager.log_event(log2, :noisy; level=:debug)
        ParallelManager.log_event(log2, :important)
        k2 = _kinds(p2)
        @test "noisy" in k2                               # :debug now kept
        @test "important" in k2
    end
end

@testset "EventLog/RunOpts: unknown level rejected" begin
    @test_throws ArgumentError ParallelManager.EventLog(tempname(); min_level=:bogus)
    @test_throws ArgumentError RunOpts(; log_level=:bogus)
end

@testset "run!: default suppresses key_start; :debug logs it" begin
    for (lvl, expect_start) in ((:info, false), (:debug, true))
        outdir = mktempdir()
        try
            v = DataVault.Vault(FIXTURE_CFG; run="phase1", outdir=outdir)
            keys = ParamIO.expand(v.spec)
            run!(k -> Dict{String,Any}("x" => 1), v, keys; opts=RunOpts(; log_level=lvl))
            logf = only(filter(f -> startswith(f, "events_"), readdir(outdir)))
            k = _kinds(joinpath(outdir, logf))
            @test "key_done" in k                          # :info — always logged
            @test "stage_done" in k                        # :info — always logged
            @test ("key_start" in k) == expect_start       # :debug — only when :debug
        finally
            rm(outdir; recursive=true, force=true)
        end
    end
end
