using ParallelManager, Test, DataVault, ParamIO, Distributed

# The behaviour this PR exists to deliver: under real Distributed workers, run! must load the seam
# packages (+ the `load=` module) in Main on every worker before fan-out. The whole existing suite
# runs at nprocs()==1 (the sequential branch), so this is the only test that enters the pmap path.
@testset "run! distributed: load= delivers a module to fresh workers" begin
    fixture = joinpath(@__DIR__, "fixtures", "study.toml")
    # Workers must share this project so the seam packages resolve; init_workers! normally sets this
    # via exeflags — here we addprocs directly with the same --project.
    proj = dirname(Base.active_project())
    pids = addprocs(2; exeflags="--project=$proj")
    try
        @test nprocs() > 1
        outdir = mktempdir()
        try
            v = DataVault.Vault(fixture; run="phase1", outdir=outdir)
            keys = ParamIO.expand(v.spec)
            @test length(keys) > 0

            # The work_fn calls `Dates.Year` — `Dates` is NOT in the auto-loaded seam and is NOT
            # loaded on a fresh worker. The keys complete ONLY if `load=:Dates` reached the workers
            # (the cryptic worker-side KeyError this PR fixes would otherwise fail every key).
            wf =
                key -> Dict{String,Any}(
                    "yr" => Dates.value(Dates.Year(2024)), "N" => key.params["N"]
                )
            result = run!(wf, v, keys; load=:Dates)

            @test result.total == length(keys)
            @test result.done == length(keys)
            @test result.err == 0
            for k in keys
                @test DataVault.is_done(v, k)
            end
        finally
            rm(outdir; recursive=true, force=true)
        end
    finally
        rmprocs(pids)
    end
end
