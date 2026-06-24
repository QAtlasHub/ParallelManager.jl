# InitWorkers — unified worker bootstrap for Threads / Distributed / SLURM.
#
# Absorbs the SlurmClusterManager + addprocs + BLAS-tuning pattern that used
# to live (in 60 lines) inside
# `Vault/.vault/templates/templateHPC.jl/src/parallel/init.jl`, so that the
# templateHPC scaffold can be reduced to a thin wrapper.

using Distributed
using LinearAlgebra
using Printf
using SlurmClusterManager

"""
    init_workers!(; mode=:auto, master_blas=1, launch_timeout=300.0,
                    worker_timeout=300, verbose=true) -> Symbol

Bootstrap worker processes / threads according to `mode`, and return the
mode actually used (useful when `mode=:auto`).

# Modes

# Timeouts (relevant to `:slurm` / `:distributed`)

- `launch_timeout::Real = 300.0` — seconds the master will wait for
  `SlurmClusterManager.SlurmManager` to produce worker addresses via
  `srun`.  Large jobs (≳ 100 workers) with cold NFS package caches need
  a substantially larger value than the SlurmManager default (60s).
- `worker_timeout::Integer = 300` — value exported as
  `JULIA_WORKER_TIMEOUT` so every freshly spawned Julia worker waits up
  to this many seconds for the master to send its first handshake
  message.  The built-in Distributed default is 60s, which is too
  tight when 100+ workers race each other through
  `_include_from_serialized` on a shared depot.

Both defaults (300s) handle the 128-worker i8cpu case on ISSP System B
comfortably.  Set lower values only for local debugging.

Idempotent: calling multiple times with worker processes already present
does not double-add. BLAS thread settings are always (re)applied.
"""
function init_workers!(;
    mode::Symbol=:auto,
    master_blas::Int=1,
    launch_timeout::Real=300.0,
    worker_timeout::Integer=300,
    verbose::Bool=true,
)
    actual = mode == :auto ? detect_mode() : mode

    if actual == :sequential
        BLAS.set_num_threads(master_blas)
        verbose && _log_init("sequential", 0, master_blas, master_blas)
        return :sequential

    elseif actual == :threads
        BLAS.set_num_threads(master_blas)
        verbose && _log_init("threads", Threads.nthreads() - 1, master_blas, master_blas)
        return :threads

    elseif actual == :distributed
        n_workers = parse(
            Int, get(ENV, "JULIA_SLURM_N_WORKERS", get(ENV, "SLURM_NTASKS", "1"))
        )
        worker_blas = parse(
            Int, get(ENV, "JULIA_WORKER_CPUS", get(ENV, "SLURM_CPUS_PER_TASK", "1"))
        )
        if n_workers > 0 && nprocs() == 1
            project = dirname(Base.active_project())
            # Export JULIA_WORKER_TIMEOUT so the freshly spawned workers
            # inherit a generous handshake window.  Distributed reads
            # this env var at worker startup only.
            withenv("JULIA_WORKER_TIMEOUT" => string(worker_timeout)) do
                return addprocs(n_workers; exeflags="--project=$project")
            end
        end
        _apply_blas(master_blas, worker_blas)
        if verbose
            _log_init("distributed", n_workers, master_blas, worker_blas)
            verify_workers!()
        end
        return :distributed

    elseif actual == :slurm
        n_workers = parse(
            Int, get(ENV, "JULIA_SLURM_N_WORKERS", get(ENV, "SLURM_NTASKS", "0"))
        )
        worker_blas = parse(
            Int, get(ENV, "JULIA_WORKER_CPUS", get(ENV, "SLURM_CPUS_PER_TASK", "1"))
        )
        if n_workers > 0 && nprocs() == 1
            project = dirname(Base.active_project())
            # BOTH the `SlurmManager()` construction AND the
            # `addprocs(mgr)` call must live inside the SAME
            # `withenv("SLURM_NTASKS" => string(n_workers))` block.
            #
            # `SlurmClusterManager` reads `ENV["SLURM_NTASKS"]` lazily
            # inside `launch(mgr, ...)` (i.e. during `addprocs`), not
            # at constructor time.  If `addprocs` runs outside the
            # withenv, SlurmManager sees the job-level `SLURM_NTASKS`
            # (= master + workers) and tries to spawn one more worker
            # than there is a task slot for.  The extra worker never
            # arrives, the master blocks forever in `addprocs`, and
            # in stdout this looks like "no output after precompile
            # ok".  See FiniteTemperature.jl's reference
            # `src/Parallel/Slurm.jl::init_slurm_workers!` for the
            # working pattern we are porting here.
            #
            # `JULIA_WORKER_TIMEOUT` is nested inside the same block so
            # srun-spawned workers inherit a longer handshake window.
            withenv(
                "SLURM_NTASKS" => string(n_workers),
                "JULIA_WORKER_TIMEOUT" => string(worker_timeout),
            ) do
                mgr = SlurmClusterManager.SlurmManager(;
                    launch_timeout=Float64(launch_timeout)
                )
                return addprocs(mgr; exeflags="--project=$project")
            end
        end
        _apply_blas(master_blas, worker_blas)
        if verbose
            _log_init("slurm", n_workers, master_blas, worker_blas)
            verify_workers!()
        end
        return :slurm

    else
        error("init_workers!: unknown mode $(actual)")
    end
end

"""
    detect_mode() -> Symbol

Inspect the environment to pick a default [`init_workers!`](@ref) mode:

- `:slurm` if `SLURM_JOB_ID` is present in `ENV`,
- `:distributed` if `JULIA_SLURM_N_WORKERS > 0` (multi-worker without a Slurm job — e.g. a local
  `addprocs` smoke test driven by the same env var the batch scripts set),
- `:threads` if `Threads.nthreads() > 1`,
- `:sequential` otherwise.

This is what `init_workers!(mode=:auto)` delegates to. Callers rarely
need to invoke `detect_mode` directly; it is public mainly for tests.
"""
function detect_mode()::Symbol
    haskey(ENV, "SLURM_JOB_ID") && return :slurm
    # Local multi-worker (Distributed): `JULIA_SLURM_N_WORKERS > 0` without a Slurm job. Recognising
    # it here lets a compute script just call `init_workers!(mode=:auto)` instead of hand-rolling the
    # `if SLURM_JOB_ID … elseif JULIA_SLURM_N_WORKERS … else …` dispatch in every project.
    # `tryparse`, not `parse`: a declared-but-empty / malformed JULIA_SLURM_N_WORKERS (some batch
    # systems export the var unset) must fall through to threads/sequential, not crash detect_mode.
    something(tryparse(Int, get(ENV, "JULIA_SLURM_N_WORKERS", "0")), 0) > 0 &&
        return :distributed
    Threads.nthreads() > 1 && return :threads
    return :sequential
end

function _apply_blas(master_blas::Int, worker_blas::Int)
    if nprocs() > 1
        _w = worker_blas
        @everywhere workers() begin
            Core.eval(Main, :(using LinearAlgebra))
        end
        @everywhere workers() BLAS.set_num_threads($_w)
    end
    BLAS.set_num_threads(master_blas)
    return nothing
end

function _log_init(mode::String, n_workers::Int, master_blas::Int, worker_blas::Int)
    # Note: this is init-time informational output, not per-item. OK to use
    # println here (single line, once per run).
    println("=== ParallelManager.init_workers! ($mode) ===")
    println("  workers     : $n_workers")
    println("  master BLAS : $master_blas")
    println("  worker BLAS : $worker_blas")
    println("  total procs : $(nprocs())")
    println("=============================================")
    return nothing
end

"""
    verify_workers!()

Probe each Distributed worker for hostname, Julia threads, BLAS threads,
and CPU affinity. Prints a summary table and emits a `@warn` if any
worker has `BLAS.get_num_threads() > 1` (a common cause of OpenBLAS
segfaults in multi-process Julia).

Ported from FiniteTemperature.jl `Parallel/Slurm.jl::print_worker_identities`.
"""
function verify_workers!()
    nprocs() > 1 || return nothing
    println("\n--- Worker verification ---")
    # The probe closure MUST be evaluated in the worker's Main module —
    # otherwise it gets serialized under ParallelManager's scope and
    # deserialization fails on workers that haven't loaded ParallelManager
    # (a common setup when compute.jl loads the package only on the master
    # before calling init_workers!).
    futures = [
        remotecall(
            Core.eval,
            p,
            Main,
            quote
                local _cpuset = "N/A"
                try
                    for line in eachline("/proc/self/status")
                        if startswith(line, "Cpus_allowed_list:")
                            _cpuset = strip(split(line, ":")[2])
                            break
                        end
                    end
                catch
                end
                (
                    Distributed.myid(),
                    Base.gethostname(),
                    Threads.nthreads(),
                    LinearAlgebra.BLAS.get_num_threads(),
                    _cpuset,
                )
            end,
        ) for p in workers()
    ]

    for f in futures
        pid, host, nth, blas, cpuset = fetch(f)
        @printf(
            "  worker %d  host=%-12s  threads=%-3d  blas=%-3d  cpus=%s\n",
            pid,
            host,
            nth,
            blas,
            cpuset
        )
        if blas > 1
            @warn "Worker $pid: BLAS threads=$blas > 1 — OpenBLAS segfault risk"
        end
    end
    println()
    flush(stdout)
    return nothing
end

# Load `modnames` (e.g. [:ParamIO, :DataVault, :ParallelManager, :MyWork]) into `Main` on EVERY
# worker, so a pmap'd task — `DataKey` deserialization, `run!`'s `acquire_running!`/`save!`/
# `mark_done!` pipeline, and the user's `work_fn` — can resolve them. `init_workers!` spawns the
# workers with `--project` but does NOT load the project's packages; without this a real
# multi-worker run dies with a cryptic `KeyError: <Module> not found` on a worker (only ever seen on
# real Slurm, never on the master, because the master loaded the packages before `addprocs`).
#
# Uses `remotecall(Core.eval, …, quoted-using-expr)` — DATA, not a module-scoped closure — so it
# works even on a worker that has not yet loaded ParallelManager (the same reason `verify_workers!`
# evaluates its probe in the worker's `Main`). Idempotent: re-`using` an already-loaded module is a
# no-op, so this composes with a project that still broadcasts modules by hand.
function _ensure_worker_modules(modnames)
    nprocs() > 1 || return nothing
    names = unique(modnames)                      # `modnames` is already a Vector{Symbol}
    isempty(names) && return nothing
    ex = Expr(:block, (Expr(:using, Expr(:., n)) for n in names)...)
    try
        @sync for w in workers()
            @async remotecall_fetch(Core.eval, w, Main, ex)
        end
    catch e
        # Surface a worker-side load failure with context instead of a bare RemoteException:
        # the usual cause is a worker whose `--project` is missing a package, which is exactly
        # the failure this function exists to make legible.
        error(
            "ParallelManager: failed to load $(names) on a worker — check the worker's " *
            "--project provides every package named by `run!(…; load=…)` plus the seam " *
            "packages (ParamIO/DataVault/ParallelManager).\nUnderlying error:\n" *
            sprint(showerror, e),
        )
    end
    return nothing
end

# Normalise the user's `load=` argument (a Module, Symbol, String, or a collection of them) to a
# `Vector{Symbol}` of module names that `_ensure_worker_modules` can `using`.
_modname(m::Module) = nameof(m)
_modname(s::Symbol) = s
_modname(s::AbstractString) = Symbol(s)
# Clear error for an unsupported `load=` entry (e.g. `load=42` or `load=[1, 2]`) instead of a
# deep `MethodError: no method matching _modname(::Int64)` with no mention of `load=`.
function _modname(x)
    return throw(
        ArgumentError(
            "`load=` must name modules (Module/Symbol/String); got a $(typeof(x))"
        ),
    )
end
_worker_module_names(::Nothing) = Symbol[]
_worker_module_names(x::Union{AbstractVector,Tuple}) = Symbol[_modname(m) for m in x]
_worker_module_names(x) = Symbol[_modname(x)]

export init_workers!, detect_mode, verify_workers!
