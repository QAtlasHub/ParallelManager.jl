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
                    worker_timeout=300, verbose=true, sysimage=<env>,
                    max_workers=<limit>, worker_logs=nothing) -> Symbol

Bootstrap worker processes / threads according to `mode`, and return the
mode actually used (useful when `mode=:auto`).

# Modes

- `:auto` — whatever [`detect_mode`](@ref) picks from the environment.
- `:sequential` — no workers; `run!` runs the keys on the master.
- `:threads` — no worker processes either: sets the master's BLAS threads and returns. `run!`
  dispatches over processes, so under `:threads` it runs the keys on the master, one at a time.
- `:distributed` — `addprocs` of `JULIA_SLURM_N_WORKERS` local workers.
- `:slurm` — one worker per Slurm task of the current job, through `SlurmClusterManager`.

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

# Start-up

- `sysimage` — a system image the workers start from (`--sysimage`), so a worker is up in seconds
  instead of loading and compiling the application. **Defaults to
  `ENV["SWEEPRUNNER_SYSIMAGE"]`.** Building one (PackageCompiler, once per commit) is the
  application's step; start the master from the same image.
- `max_workers` — the most workers ONE master may start under `:slurm`. Every worker there is an
  `srun` client on the master's node and takes about seven ports of the cluster's
  `SrunPortRange`; past the range, further workers neither join nor fail, and the job runs at a
  fraction of its cores to the end. Asking for more than the limit is an error that says so,
  instead of that stall. Defaults to `ENV["SWEEPRUNNER_MAX_WORKERS"]`, else what
  [`srun_worker_limit`](@ref) reads from the cluster, else no limit. Above it, run several
  masters on node groups ([`split_nodes`](@ref)) with `SWEEPRUNNER_SHARD=i/m`.
- `worker_logs` — a directory: every worker writes its own stdout / stderr to a file there
  ([`worker_logs!`](@ref)) instead of relaying each line through the master.

Idempotent: calling multiple times with worker processes already present
does not double-add. BLAS thread settings are always (re)applied.
"""
function init_workers!(;
    mode::Symbol=:auto,
    master_blas::Int=1,
    launch_timeout::Real=300.0,
    worker_timeout::Integer=300,
    verbose::Bool=true,
    sysimage::Union{AbstractString,Nothing}=get(ENV, "SWEEPRUNNER_SYSIMAGE", nothing),
    max_workers::Union{Integer,Nothing}=_max_workers_default(),
    worker_logs::Union{AbstractString,Nothing}=nothing,
)
    actual = mode == :auto ? detect_mode() : mode
    flags = _worker_exeflags(sysimage)

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
            # What the status compares the workers that actually join against.
            note_workers!(; planned=n_workers, launched=n_workers)
            project = dirname(Base.active_project())
            # Export JULIA_WORKER_TIMEOUT so the freshly spawned workers
            # inherit a generous handshake window.  Distributed reads
            # this env var at worker startup only.
            withenv("JULIA_WORKER_TIMEOUT" => string(worker_timeout)) do
                return addprocs(n_workers; exeflags=`--project=$project $flags`)
            end
            worker_logs === nothing || worker_logs!(worker_logs)
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
            _check_worker_limit(n_workers, max_workers)
            # What the status compares the workers that actually join against.
            note_workers!(; planned=n_workers, launched=n_workers)
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
                return addprocs(mgr; exeflags=`--project=$project $flags`)
            end
            worker_logs === nothing || worker_logs!(worker_logs)
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
    println("=== SweepRunner.init_workers! ($mode) ===")
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
and CPU affinity. Prints a summary table, then ONE `@warn` carrying how many
workers have `BLAS.get_num_threads() > 1`.

That setting is reported, not diagnosed. It is a known cause of OpenBLAS
segfaults in multi-process Julia, but on a 2-site TDVP workload (10 sites,
chi=20) ms/step was flat from 1 to 36 threads and ~250 completed keys at
`blas=16` produced no segfault, so the warning does not claim the setting is
wrong here. It used to fire per worker: 287 lines on a 72-node allocation,
interleaved with the rows of the table above it.

Ported from FiniteTemperature.jl `Parallel/Slurm.jl::print_worker_identities`.
"""
function verify_workers!()
    nprocs() > 1 || return nothing
    println("\n--- Worker verification ---")
    # The probe closure MUST be evaluated in the worker's Main module —
    # otherwise it gets serialized under SweepRunner's scope and
    # deserialization fails on workers that haven't loaded SweepRunner
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

    nhot = 0
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
        blas > 1 && (nhot += 1)
    end
    # One line, after the table rather than interleaved with it. Per worker this was 287 lines on
    # a 72-node allocation, which is the table's readability spent on a risk that has not been
    # measured on this workload.
    nhot > 0 &&
        @warn "$nhot of $(nworkers()) workers have BLAS threads > 1 (OpenBLAS segfault risk under multi-process Julia). Set OPENBLAS_NUM_THREADS=1 if you hit one." maxlog =
            1
    println()
    flush(stdout)
    return nothing
end

# Load `modnames` (e.g. [:ParamIO, :DataVault, :SweepRunner, :MyWork]) into `Main` on EVERY
# worker, so a dispatched key — `DataKey` deserialization, `run!`'s `acquire_running!`/`save!`/
# `mark_done!` pipeline, and the user's `work_fn` — can resolve them. `init_workers!` spawns the
# workers with `--project` but does NOT load the project's packages; without this a real
# multi-worker run dies with a cryptic `KeyError: <Module> not found` on a worker (only ever seen on
# real Slurm, never on the master, because the master loaded the packages before `addprocs`).
#
# Uses `remotecall(Core.eval, …, quoted-using-expr)` — DATA, not a module-scoped closure — so it
# works even on a worker that has not yet loaded SweepRunner (the same reason `verify_workers!`
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
            "SweepRunner: failed to load $(names) on a worker — check the worker's " *
            "--project provides every package named by `run!(…; load=…)` plus the seam " *
            "packages (ParamIO/DataVault/SweepRunner).\nUnderlying error:\n" *
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

# ── start-up: the image, the limit, the node groups, the logs ───────────────────────────────────

# Extra command-line flags for a worker: the system image, when there is one.
function _worker_exeflags(sysimage)
    (sysimage === nothing || isempty(sysimage)) && return String[]
    isfile(sysimage) || throw(ArgumentError("init_workers!: no system image at $sysimage"))
    return ["--sysimage=$(abspath(sysimage))"]
end

# About how many ports one `srun` client holds on the master's node.
const _PORTS_PER_SRUN = 7

"""
    srun_worker_limit(config_text) -> Union{Int,Nothing}
    srun_worker_limit() -> Union{Int,Nothing}

How many workers one master can start as `srun` job steps: the size of the cluster's
`SrunPortRange` (from `scontrol show config`, or from the text given) divided by the ports one
`srun` takes, with a tenth held back. `nothing` when the range is not configured or cannot be
read — then there is no known limit.

On a cluster with `SrunPortRange = 52501-65000` this is 1607: the master that planned 3735
workers and stalled without an error at 1782 was past it.
"""
function srun_worker_limit(config_text::AbstractString)
    m = match(r"SrunPortRange\s*=\s*(\d+)\s*-\s*(\d+)", config_text)
    m === nothing && return nothing
    lo, hi = parse(Int, m[1]), parse(Int, m[2])
    hi > lo || return nothing
    return floor(Int, 0.9 * (hi - lo + 1) / _PORTS_PER_SRUN)
end

function srun_worker_limit()
    Sys.which("scontrol") === nothing && return nothing
    out = try
        _run_command(`scontrol show config`; timeout=20.0)
    catch e
        e isa InterruptException && rethrow()
        nothing
    end
    return out === nothing ? nothing : srun_worker_limit(out)
end

function _max_workers_default()
    n = tryparse(Int, get(ENV, "SWEEPRUNNER_MAX_WORKERS", ""))
    n !== nothing && return n
    # Only inside an allocation: `scontrol` elsewhere may answer for another cluster.
    return haskey(ENV, "SLURM_JOB_ID") ? srun_worker_limit() : nothing
end

function _check_worker_limit(n::Integer, limit)
    (limit === nothing || n <= limit) && return nothing
    return throw(
        ArgumentError(
            "init_workers!: $n workers asked of one master, and the limit here is $limit " *
            "(each worker is an srun client on the master's node and takes about " *
            "$(_PORTS_PER_SRUN) ports of SrunPortRange; past it workers neither join nor " *
            "fail). Run $(cld(n, limit)) masters on node groups instead — " *
            "`split_nodes(nodes, m)`, one master per group with SWEEPRUNNER_SHARD=i/m — or " *
            "raise the limit with `max_workers=` / SWEEPRUNNER_MAX_WORKERS if it is wrong.",
        ),
    )
end

"""
    split_nodes(nodes, m) -> Vector{Vector{String}}

Cut a node list into `m` groups of as equal size as possible, in order — one group per master of
a large allocation. Start-up rate and every per-master limit then scale with the allocation:

```sh
# m masters as job steps, each on its own group, each starting on its own share of the keys
srun -N \$n_i -w \$group_i --export=ALL,SWEEPRUNNER_SHARD=\$i/\$m julia run_campaign.jl &
```

`nodes` is a vector of host names, or a Slurm node list (`c[001-072]`).
"""
function split_nodes(nodes::AbstractVector{<:AbstractString}, m::Integer)
    m >= 1 || throw(ArgumentError("split_nodes: m must be >= 1, got $m"))
    n = length(nodes)
    groups = Vector{String}[]
    start = 1
    for i in 1:m
        len = div(n, m) + (i <= rem(n, m) ? 1 : 0)
        push!(groups, String.(nodes[start:(start + len - 1)]))
        start += len
    end
    return filter!(!isempty, groups)
end

function split_nodes(nodelist::AbstractString, m::Integer)
    return split_nodes(expand_nodelist(nodelist), m)
end

# Where workers that join later should send their output (set by `worker_logs!`).
const _WORKER_LOG_DIR = Ref{Union{String,Nothing}}(nothing)

# Run ON a worker: send this process's stdout and stderr to its own file.
function _redirect_output(dir::AbstractString)
    mkpath(dir)
    path = joinpath(dir, "worker_$(gethostname())_$(getpid()).log")
    io = open(path, "a")
    redirect_stdout(io)
    redirect_stderr(io)
    return path
end

"""
    worker_logs!(dir; pids=workers()) -> Dict{Int,String}

Make every worker write its stdout and stderr to its own file,
`<dir>/worker_<host>_<pid>.log`, instead of sending each line to the master to be printed with a
`From worker N:` prefix. On a large job that relay is one interleaved stream from thousands of
workers through the master's one core, and the job log is the only place a worker's messages
exist. Returns worker id => file. Workers that join later ([`run!`](@ref) adopts them) are
redirected to the same directory.
"""
function worker_logs!(dir::AbstractString; pids=workers())
    dir = abspath(dir)
    _WORKER_LOG_DIR[] = dir
    out = Dict{Int,String}()
    for p in pids
        p == myid() && continue
        out[p] = remotecall_fetch(_redirect_output, p, dir)
    end
    return out
end

export init_workers!, detect_mode, verify_workers!
export srun_worker_limit, split_nodes, worker_logs!
