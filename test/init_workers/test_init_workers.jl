using SweepRunner, Test, Distributed
using LinearAlgebra

@testset "detect_mode: no SLURM" begin
    # Strip SLURM env for the duration of this test
    withenv("SLURM_JOB_ID" => nothing) do
        m = detect_mode()
        @test m in (:threads, :sequential)
    end
end

@testset "detect_mode: SLURM_JOB_ID present" begin
    withenv("SLURM_JOB_ID" => "12345") do
        @test detect_mode() == :slurm
    end
end

@testset "init_workers!: :sequential is a no-op" begin
    withenv("SLURM_JOB_ID" => nothing) do
        m = init_workers!(mode=:sequential, master_blas=1, verbose=false)
        @test m == :sequential
        @test BLAS.get_num_threads() == 1
    end
end

@testset "init_workers!: :threads sets BLAS" begin
    m = init_workers!(mode=:threads, master_blas=1, verbose=false)
    @test m == :threads
    @test BLAS.get_num_threads() == 1
end

@testset "init_workers!: idempotent on re-call" begin
    init_workers!(mode=:sequential, master_blas=2, verbose=false)
    init_workers!(mode=:sequential, master_blas=2, verbose=false)
    @test BLAS.get_num_threads() == 2
    init_workers!(mode=:sequential, master_blas=1, verbose=false)  # restore
end

@testset "init_workers!: :auto with no SLURM" begin
    withenv("SLURM_JOB_ID" => nothing) do
        m = init_workers!(mode=:auto, verbose=false)
        @test m in (:threads, :sequential)
    end
end

@testset "init_workers!: unknown mode errors" begin
    @test_throws ErrorException init_workers!(mode=:nonsense, verbose=false)
end

@testset "init_workers!: :slurm env reading (no actual addprocs)" begin
    # We cannot truly spawn a SLURM worker here. Instead, set
    # JULIA_SLURM_N_WORKERS=0 so addprocs is skipped but the code path runs.
    withenv(
        "SLURM_JOB_ID" => "1", "JULIA_SLURM_N_WORKERS" => "0", "JULIA_WORKER_CPUS" => "2"
    ) do
        m = init_workers!(mode=:slurm, verbose=false)
        @test m == :slurm
        # Master BLAS was set (worker count is 0, no @everywhere)
        @test BLAS.get_num_threads() >= 1
    end
    init_workers!(mode=:sequential, master_blas=1, verbose=false)  # restore
end

@testset "detect_mode: JULIA_SLURM_N_WORKERS>0 without SLURM_JOB_ID => :distributed" begin
    withenv("SLURM_JOB_ID" => nothing, "JULIA_SLURM_N_WORKERS" => "2") do
        @test detect_mode() == :distributed
    end
end

@testset "detect_mode: SLURM_JOB_ID outranks JULIA_SLURM_N_WORKERS" begin
    withenv("SLURM_JOB_ID" => "99", "JULIA_SLURM_N_WORKERS" => "4") do
        @test detect_mode() == :slurm
    end
end

@testset "detect_mode: malformed JULIA_SLURM_N_WORKERS does not crash" begin
    # A declared-but-empty / non-integer value must fall through, never throw.
    withenv("SLURM_JOB_ID" => nothing, "JULIA_SLURM_N_WORKERS" => "") do
        @test detect_mode() in (:threads, :sequential)
    end
    withenv("SLURM_JOB_ID" => nothing, "JULIA_SLURM_N_WORKERS" => "notanint") do
        @test detect_mode() in (:threads, :sequential)
    end
end

@testset "_worker_module_names / _modname normalization" begin
    wmn = SweepRunner._worker_module_names
    @test wmn(nothing) == Symbol[]
    @test wmn(:Statistics) == [:Statistics]
    @test wmn("Statistics") == [:Statistics]
    @test wmn(LinearAlgebra) == [:LinearAlgebra]      # Module dispatch
    @test wmn([:A, :B]) == [:A, :B]
    @test wmn(("A", :B)) == [:A, :B]                  # Tuple, mixed String/Symbol
    @test_throws ArgumentError wmn(42)                # clear error, not a deep MethodError
    @test_throws ArgumentError wmn([1, 2])            # bad collection element
end

@testset "the :slurm backend's step is not ended by one worker being killed" begin
    # All workers are tasks of one `srun` step; with `KillOnBadExit=1` in the cluster's
    # configuration Slurm ends the whole step when one of them is killed.
    env = Dict(SweepRunner._slurm_launch_env(16, 300, Dict{String,String}()))
    @test env["SLURM_KILL_BAD_EXIT"] == "0"
    @test env["SLURM_NTASKS"] == "16"
    @test env["JULIA_WORKER_TIMEOUT"] == "300"
    # What the job's own script chose is not overridden.
    mine = Dict("SLURM_KILL_BAD_EXIT" => "1")
    @test Dict(SweepRunner._slurm_launch_env(4, 60, mine))["SLURM_KILL_BAD_EXIT"] == "1"
    # ...and it is what `init_workers!` starts `srun` in: the launch is stood in for, and
    # reads the environment it is called in.
    if nprocs() == 1
        seen = Ref{Any}(nothing)
        SweepRunner._SLURM_LAUNCH[] =
            (timeout, flags) -> begin
                seen[] = (ENV["SLURM_KILL_BAD_EXIT"], ENV["SLURM_NTASKS"])
                Int[]
            end
        start = () -> init_workers!(; mode=:slurm, verbose=false, max_workers=nothing)
        try
            withenv(start, "JULIA_SLURM_N_WORKERS" => "3", "SLURM_KILL_BAD_EXIT" => nothing)
            @test seen[] == ("0", "3")
            withenv(start, "JULIA_SLURM_N_WORKERS" => "3", "SLURM_KILL_BAD_EXIT" => "1")
            @test seen[] == ("1", "3")
        finally
            SweepRunner._SLURM_LAUNCH[] = nothing
            note_workers!(; planned=0, launched=0)
        end
        @test !haskey(ENV, "SLURM_NTASKS") || ENV["SLURM_NTASKS"] != "3"   # not left set
    end
end
