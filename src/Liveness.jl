# Liveness — is the master that holds this lock still running?
#
# `stale_after` answers that by waiting. It has to, because a heartbeat can only ever say
# "recently alive"; the absence of one is indistinguishable from a slow filesystem until enough
# time has passed. Where the question CAN be asked outright, asking is worth several minutes of a
# batch allocation.
#
# Nothing here is required for correctness: every path degrades to `:unknown`, and `stale_after`
# then decides exactly as before. `:slurm` is one of four worker modes, so a mechanism that only
# worked there would cover a quarter of the runs.

"""
    owner_token() -> String

The identity of ONE acquisition, stamped into `.running` by [`run!`](@ref) so a sibling can ask
about it later. `host:pid:nonce` from DataVault, with `:slurm<jobid>` appended inside a Slurm
allocation.

Call it per acquisition and do not cache it. The nonce is what tells a master's current hold from
a hold it lost and retook, and a token broadcast once per process cannot make that distinction.

The Slurm field is what makes the question answerable ACROSS hosts: `/proc` only works for a
holder on this machine, and the master of the job that was killed is usually somewhere else.
"""
function owner_token()::String
    base = DataVault.new_owner_token()
    job = _slurm_queue_id()
    return isempty(job) ? base : string(base, ":slurm", job)
end

"""
    owner_token(host, pid) -> String

A token for an acquisition that the process `pid` on `host` is about to make, written by someone
else: the master, naming the lock its worker will take, so that it knows the name without asking.
Same shape as `owner_token()`, and the Slurm field is this process's — a master and its workers
share a job.
"""
function owner_token(host::AbstractString, pid::Integer)::String
    base = string(host, ':', pid, ':', string(rand(UInt32); base=16, pad=8))
    job = _slurm_queue_id()
    return isempty(job) ? base : string(base, ":slurm", job)
end

# The id as `squeue` PRINTS it, which is not always `SLURM_JOB_ID`. In an array task that variable
# holds a distinct raw id per task while the queue lists `<array id>_<task index>`, so stamping it
# makes every task but the first unfindable, and unfindable reads as `:dead`.
function _slurm_queue_id()::String
    arr = get(ENV, "SLURM_ARRAY_JOB_ID", "")
    idx = get(ENV, "SLURM_ARRAY_TASK_ID", "")
    (isempty(arr) || isempty(idx)) && return get(ENV, "SLURM_JOB_ID", "")
    return string(arr, "_", idx)
end

"""
    holder_liveness(owner) -> Symbol

`:alive`, `:dead`, or `:unknown` for the holder named by an [`owner_token`](@ref).

`:dead` is only ever returned on positive evidence that the process is gone. Everything else is
`:unknown`, including every error path: a wrong `:dead` would hand a live master's key to someone
else, which is the one outcome the lock exists to prevent.

Two sources, in order:

1. a Slurm job id, when this process is ITSELF inside a Slurm allocation. The job is absent from
   the queue, so it has finished, been cancelled, or hit its wall clock.
2. the pid, when the holder is on THIS host and `/proc` exists. A recycled pid reads as `:alive`,
   which is the safe direction.
"""
function holder_liveness(owner::AbstractString)::Symbol
    parts = split(String(owner), ':')
    length(parts) >= 3 || return :unknown

    # The Slurm field is the FOURTH part and nowhere else, because that is where `owner_token`
    # writes it. Searching every part for a `slurm` prefix matched a HOSTNAME beginning with
    # `slurm`, read the rest of the hostname as a job id, found it absent from the queue, and
    # returned `:dead` for a master that was alive.
    if length(parts) >= 4 && startswith(parts[4], "slurm")
        s = _slurm_liveness(chopprefix(parts[4], "slurm"))
        s === :unknown || return s
    end

    parts[1] == gethostname() || return :unknown
    pid = tryparse(Int, parts[2])
    pid === nothing && return :unknown
    return _pid_liveness(pid)
end

# `/proc/<pid>` is the whole check on Linux. Elsewhere there is no equally cheap answer that does
# not risk a false `:dead`, so there is no answer.
function _pid_liveness(pid::Int)::Symbol
    Sys.islinux() || return :unknown
    return isdir("/proc/$(pid)") ? :alive : :dead
end

# The live-job set, cached: `squeue` is a scheduler RPC and this is consulted per contended key.
const _SQUEUE_TTL = 15.0
const _SQUEUE_TIMEOUT = 10.0
const _squeue_cache = Ref{Tuple{Float64,Union{Set{String},Nothing}}}((-Inf, nothing))
const _squeue_lock = ReentrantLock()

function _slurm_liveness(jobid::AbstractString)::Symbol
    isempty(jobid) && return :unknown
    # A job id Slurm would never issue cannot be looked up, and "not in the queue" must not be the
    # answer for it: that is a `:dead` conjured out of a corrupt `.running` file. `_` is legal
    # because an array task's queue id is `<array id>_<task index>`.
    all(c -> isdigit(c) || c == '_', jobid) || return :unknown
    # Only from INSIDE an allocation: a `squeue` on PATH may be a wrapper pointed at an unrelated
    # cluster, where every foreign job id is absent and would therefore read as `:dead`.
    haskey(ENV, "SLURM_JOB_ID") || return :unknown
    live = _live_slurm_jobs()
    live === nothing && return :unknown
    # The queue has to be able to see THIS process before its silence about anyone else means
    # anything. A `squeue` pointed at a different cluster answers successfully and lists none of
    # our ids, and every holder would then read as `:dead`.
    _slurm_queue_id() in live || return :unknown
    # An array task is `12345_7` in the queue while `SLURM_JOB_ID` is `12345`, so the whole job is
    # alive if any of its tasks is.
    jobid in live && return :alive
    any(j -> startswith(j, jobid * "_"), live) && return :alive
    return :dead
end

# Bounded, because this runs on the hot path of every contended key and `squeue` can be a wrapper
# around a network call. An unbounded one that stalls holds `_squeue_lock` and takes the allocation
# with it, and neither `stop_flag` nor `deadline` is read inside a key.
function _squeue_output(timeout::Real)::Union{String,Nothing}
    Sys.which("squeue") === nothing && return nothing
    tmp = tempname()
    proc = try
        run(pipeline(`squeue -h -o %i`; stdout=tmp, stderr=devnull); wait=false)
    catch e
        e isa InterruptException && rethrow()
        rm(tmp; force=true)
        return nothing
    end
    try
        if timedwait(() -> !process_running(proc), Float64(timeout); pollint=0.05) !== :ok
            kill(proc, Base.SIGKILL)
            return nothing
        end
        success(proc) || return nothing
        return read(tmp, String)
    finally
        rm(tmp; force=true)
    end
end

# `nothing` means "no opinion": squeue is absent, timed out, or failed. Listing the queue and testing
# membership is deliberate. `squeue -j <id>` exits non-zero BOTH for a finished job and for a
# broken scheduler, and those must not collapse into the same answer.
function _live_slurm_jobs()::Union{Set{String},Nothing}
    return lock(_squeue_lock) do
        t, cached = _squeue_cache[]
        time() - t < _SQUEUE_TTL && return cached
        fresh = try
            out = _squeue_output(_SQUEUE_TIMEOUT)
            out === nothing ? nothing : Set(String.(split(out; keepempty=false)))
        catch e
            e isa InterruptException && rethrow()
            nothing
        end
        _squeue_cache[] = (time(), fresh)
        return fresh
    end
end

# `SLURM_JOB_CPUS_PER_NODE` (`128(x72)`, `64(x2),32`) as one count per node, or `nothing` when
# it is not in that form. The ONE reading of it: the status, the account and the pool used to
# parse it separately, one answering 0 and another throwing on the same value.
function _slurm_cpus_per_node(s::AbstractString)::Union{Vector{Int},Nothing}
    out = Int[]
    for part in split(s, ','; keepempty=false)
        m = match(r"^(\d+)(?:\(x(\d+)\))?$", strip(part))
        m === nothing && return nothing
        append!(out, fill(parse(Int, m[1]), m[2] === nothing ? 1 : parse(Int, m[2])))
    end
    return out
end

export owner_token, holder_liveness
