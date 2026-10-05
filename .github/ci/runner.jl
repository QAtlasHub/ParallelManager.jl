#!/usr/bin/env julia
# .github/ci/runner.jl — TOML 設定に基づいてテスト環境を構築・実行
#
# Usage:
#   julia --project=. --threads=4 .github/ci/runner.jl .github/ci/env_threaded.toml

using TOML, Test

cfg = TOML.parsefile(ARGS[1])
env = cfg["env"]
mode = env["mode"]
n_workers = get(env, "n_workers", 0)
n_masters = get(env, "n_masters", 1)
test_dirs = env["test_dirs"]

println("=" ^ 60)
println("  CI Environment: $(ARGS[1])")
println(
    "  mode=$mode  workers=$n_workers  masters=$n_masters  threads=$(Threads.nthreads())"
)
println("=" ^ 60)

# Export n_masters so test code can scale Threads.@spawn accordingly
ENV["PM_TEST_N_MASTERS"] = string(n_masters)

# Launch Distributed workers when mode == "distributed"
using Distributed  # always load; needed for nprocs() etc. in tests

# `preload_workers` controls whether we do `@everywhere using SweepRunner`.
# Some production Slurm setups load the package only on the master, which
# previously masked a bug in verify_workers! (PkgId not found on workers).
# Keep at least one env that tests the master-only scenario.
preload_workers = get(env, "preload_workers", true)

if mode == "distributed" && n_workers > 0
    project = dirname(Base.active_project())
    addprocs(n_workers; exeflags="--project=$project")
    if preload_workers
        @everywhere using SweepRunner, DataVault, ParamIO
        println("  Workers launched (with PM preloaded): $(workers())")
    else
        println("  Workers launched (MASTER-ONLY: PM not loaded on workers): $(workers())")
    end
end

# Load the package (must come after addprocs so @everywhere sees it)
using SweepRunner

# Run test files from the specified directories
@testset "SweepRunner ($mode, m=$(n_masters), w=$(n_workers))" begin
    for dir in test_dirs
        dirpath = joinpath(pkgdir(SweepRunner), "test", dir)
        if !isdir(dirpath)
            @warn "Test directory not found: $dirpath"
            continue
        end
        files = sort(
            filter(f -> startswith(f, "test_") && endswith(f, ".jl"), readdir(dirpath))
        )
        if isempty(files)
            @warn "No test files found in $dirpath"
            continue
        end
        for f in files
            @testset "$dir/$f" begin
                filepath = joinpath(dirpath, f)
                println("  Including $filepath")
                @time include(filepath)
            end
        end
    end
end

# Clean up workers
if nprocs() > 1
    rmprocs(workers())
end
