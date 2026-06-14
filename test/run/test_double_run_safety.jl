using ParallelManager, Test, DataVault, ParamIO

isdefined(@__MODULE__, :FIXTURE_CFG) ||
    (const FIXTURE_CFG = joinpath(@__DIR__, "fixtures", "study.toml"))

"""
test_double_run_safety.jl — RunOpts invariant enforcement, the `workers`
field wiring, and the lost-lock abort that prevents a double-commit.
"""

@testset "RunOpts: enforces heartbeat_interval < stale_after" begin
    @test_throws ArgumentError RunOpts(; heartbeat_interval=10.0, stale_after=5.0)
    @test_throws ArgumentError RunOpts(; heartbeat_interval=5.0, stale_after=5.0)
    @test RunOpts(; heartbeat_interval=5.0, stale_after=10.0) isa RunOpts
end

@testset "RunOpts: validates workers mode" begin
    @test_throws ArgumentError RunOpts(; workers=:bogus)
    @test RunOpts(; workers=:sequential) isa RunOpts
    @test RunOpts(; workers=:auto) isa RunOpts
end

@testset "run!: workers=:sequential completes" begin
    outdir = mktempdir()
    try
        v = DataVault.Vault(FIXTURE_CFG; run="phase1", outdir=outdir)
        keys = ParamIO.expand(v.spec)
        r = run!(
            k -> Dict{String,Any}("ok" => 1), v, keys; opts=RunOpts(; workers=:sequential)
        )
        @test r.done == length(keys)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

@testset "lost lock: result discarded, key not marked done (no double-commit)" begin
    outdir = mktempdir()
    try
        v = DataVault.Vault(FIXTURE_CFG; run="phase1", outdir=outdir)
        key = ParamIO.expand(v.spec)[1]
        lost = Threads.Atomic{Bool}(true)        # simulate: heartbeat saw a reclaim
        log = ParallelManager.EventLog(joinpath(outdir, "ev.jsonl"))
        ran = Ref(false)
        wf = k -> (ran[]=true; Dict{String,Any}("x" => 1))
        outcome = ParallelManager._run_one_with_retry!(
            wf, v, key, ParamIO.canonical(key), :phase1, log, RunOpts(), lost
        )
        @test ran[]                              # work_fn ran...
        @test outcome === :lock_busy             # ...but its result was discarded
        @test !DataVault.is_done(v, key)         # NOT marked done (no save!)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

@testset "error strings are truncated for the JSONL event log" begin
    s = ParallelManager._short_err(ErrorException("x"^5000))
    @test length(s) <= 2100
    @test occursin("truncated", s)
end
