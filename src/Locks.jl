# Locks — is this `.running` real, asked of someone who knows.
#
# Whether a lock was real used to be inferred, by whoever happened to visit the key, from two
# indirect signs: the heartbeat's age against `stale_after`, and the owner's scheduler job id
# against `squeue`. A live job proves nothing about a key (a worker killed inside it leaves a lock
# "held by a live job"), a fresh heartbeat proves little (a child process writes it), and locks of
# jobs that had ended stayed on disk until someone tripped over them.
#
# The holder's MASTER knows. It named the lock (`owner_token(host, pid)` at dispatch) and it says
# which tokens it has out in its status file (`held`), so the question becomes: does a master that
# knows this process say it holds this token?
#
# Order of evidence, strongest first, and `:dead` only ever on a positive answer:
#   1. this process handed the token out and has not settled it          -> :held
#   2. a master that is reporting lists the token                        -> :held
#   3. a master that knows the holder process, reporting AFTER the lock's
#      last heartbeat, does not list it                                  -> :dead
#   4. the scheduler / pid (`holder_liveness`)                           -> :dead, or :held while
#                                                                           the heartbeat is fresh
#   5. the heartbeat's age                                               -> :stale or :unknown

using DataVault
using ParamIO: DataKey, canonical

# ── what this process has out ───────────────────────────────────────────────────────────────────

# token => (vault, key) for every lock a master in THIS process has named and not yet settled.
# Process-global, because several masters can share a process (and so a status file): each one's
# status has to list all of them, or a sibling in the same process would read as "not held".
const _OUT = Dict{String,Tuple{Vault,DataKey}}()
const _OUT_LOCK = ReentrantLock()
const _EXIT_HOOK = Ref(false)

function _out_add!(tok::AbstractString, vault::Vault, key::DataKey)
    lock(_OUT_LOCK) do
        _OUT[String(tok)] = (vault, key)
        if !_EXIT_HOOK[]
            _EXIT_HOOK[] = true
            atexit(_release_all_at_exit)
        end
    end
    return nothing
end

function _out_remove!(tok::AbstractString)
    return (lock(() -> delete!(_OUT, String(tok)), _OUT_LOCK); nothing)
end

_out_tokens() = lock(() -> collect(keys(_OUT)), _OUT_LOCK)

_out_has(tok::AbstractString) = lock(() -> haskey(_OUT, String(tok)), _OUT_LOCK)

# A master that is leaving says so: the locks it named and still has out are released, so they do
# not sit on disk until `stale_after` (or until someone asks the scheduler about a job that is
# gone). Runs on a normal exit and on the scheduler's SIGTERM at the wall clock; a `kill -9` or a
# lost node still leaves orphans, which is what reconciliation is for. Owner-checked, so it
# removes nothing a sibling has since reclaimed.
function _release_all_at_exit()
    # Not `lock`: an exit hook that blocks on a lock held by a task that will never run again
    # turns a clean exit into a hang.
    trylock(_OUT_LOCK) || return nothing
    try
        for (tok, (vault, key)) in _OUT
            try
                DataVault.clear_running!(vault, key, tok)
            catch
            end
        end
        empty!(_OUT)
    finally
        unlock(_OUT_LOCK)
    end
    return nothing
end

# ── the verdict ─────────────────────────────────────────────────────────────────────────────────

"""
    LockInfo

One `.running` lock and what is known about it.

- `key` — the canonical key, or the lock file's path under the status directory when the lock was
  found by walking it.
- `owner` — the token in the file (`nothing` for an unstamped lock); `host`, `pid`, `job` are its
  parts.
- `age` — seconds since the last heartbeat.
- `verdict` — `:held` (a holder that is alive says, or is shown, to have it), `:dead` (positive
  evidence the holder is gone: safe to remove), `:stale` (no evidence either way and the
  heartbeat is older than `stale_after`: reclaimable), `:unknown` (no evidence, heartbeat fresh).
- `why` — the evidence, in words.
- `master` — the master that answered, when one did.
- `progress_age` — seconds since the computation last reported progress on this key, when known.
  A fresh heartbeat with an old progress stamp is "alive but not advancing".
"""
struct LockInfo
    key::String
    owner::Union{String,Nothing}
    host::String
    pid::Int
    job::String
    age::Float64
    verdict::Symbol
    why::String
    master::Union{String,Nothing}
    progress_age::Union{Float64,Nothing}
end

# host, pid, job out of `host:pid:nonce[:slurm<job>]`. Missing parts are "" / 0.
function _token_parts(owner::Union{AbstractString,Nothing})
    owner === nothing && return ("", 0, "")
    parts = split(String(owner), ':')
    length(parts) >= 3 || return ("", 0, "")
    pid = something(tryparse(Int, parts[2]), 0)
    job = if length(parts) >= 4 && startswith(parts[4], "slurm")
        String(chopprefix(parts[4], "slurm"))
    else
        ""
    end
    return (String(parts[1]), pid, job)
end

# How much later than the lock's last heartbeat a master's report has to be before "it does not
# list this token" means anything. Covers clock skew between the two hosts; the report and the
# heartbeat are written by different processes.
const _ASK_MARGIN = 5.0

# Does master status `m` know the process `host:pid` — as itself, or as one of its workers?
function _master_knows(m::AbstractDict, host::AbstractString, pid::Int)
    (m["host"] == host && m["pid"] == pid) && return true
    return any(r -> r["host"] == host && r["pid"] == pid, m["worker_table"])
end

"""
    judge_lock(owner, age, masters; stale_after=600.0) -> (verdict, why, master)

The verdict on a lock held as `owner` whose last heartbeat is `age` seconds old, given the
`masters` statuses ([`read_status`](@ref)). See [`LockInfo`](@ref) for the verdicts.

`:dead` needs a positive answer. A master that is reporting (or that said it ended), that knows
the holder process, and whose report was written after the lock's last heartbeat, either lists the
token or does not: a master names every lock before its worker takes it and lists it until the
worker has released it, so "does not list it" means nobody is on that key. A master that has gone
silent is not asked; the scheduler is, and after it the heartbeat's age.
"""
function judge_lock(
    owner::Union{AbstractString,Nothing},
    age::Real,
    masters::AbstractVector;
    stale_after::Real=600.0,
)
    by_age = if age > stale_after
        (:stale, "no holder answered; heartbeat $(round(Int, age)) s old", nothing)
    else
        (:unknown, "no holder answered; heartbeat fresh", nothing)
    end
    owner === nothing && return by_age
    _out_has(owner) && return (:held, "handed out by this process", nothing)
    host, pid, _ = _token_parts(owner)
    heartbeat_at = time() - age
    for m in masters
        m["stale"] && continue                       # gone silent: cannot be asked
        held = get(m, "held", nothing)
        held === nothing && continue                 # written before masters listed their locks
        if owner in held
            m["state"] == "ended" && continue        # listed by a master that has left
            return (:held, "master $(m["master"]) has it out", m["master"])
        end
        _master_knows(m, host, pid) || continue
        if Float64(m["updated"]) > heartbeat_at + _ASK_MARGIN
            return (
                :dead,
                "master $(m["master"]) knows $host:$pid and does not have this lock out",
                m["master"],
            )
        end
    end
    alive = holder_liveness(owner)
    alive === :dead && return (:dead, "holder is gone (scheduler / pid)", nothing)
    # A holder that looks alive with a heartbeat that has stopped is not evidence of a hold: the
    # heartbeat is a child of the holder, and a pid can be recycled. The age decides, as it
    # always did.
    (alive === :alive && age <= stale_after) &&
        return (:held, "holder process is alive", nothing)
    return by_age
end

# ── listing ─────────────────────────────────────────────────────────────────────────────────────

# token => seconds since progress was last reported on the key it holds, from the worker tables.
function _progress_ages(masters::AbstractVector)
    out = Dict{String,Float64}()
    for m in masters
        for r in m["worker_table"]
            (haskey(r, "owner") && get(r, "progress_at", nothing) !== nothing) || continue
            out[r["owner"]] = time() - Float64(r["progress_at"])
        end
    end
    return out
end

function _lock_info(key, owner, age, masters, pages, stale_after)
    verdict, why, master = judge_lock(owner, age, masters; stale_after=stale_after)
    host, pid, job = _token_parts(owner)
    page = owner === nothing ? nothing : get(pages, owner, nothing)
    return LockInfo(String(key), owner, host, pid, job, age, verdict, why, master, page)
end

# A key's lock as it is now: `(; owner, age)`, or `nothing` when there is none — also when it
# was released between the reads (the age is not finite then). The one place a key's `.running`
# is read to be judged; the listing, the reaper and the scan all ask here.
function _lock_now(vault::Vault, key::DataKey)
    DataVault.is_running(vault, key) || return nothing
    owner = DataVault.running_owner(vault, key)
    age = DataVault.running_age_secs(vault, key)
    isfinite(age) || return nothing
    return (; owner, age)
end

"""
    locks(vault, keys; stale_after=600.0) -> Vector{LockInfo}
    locks(vault; stale_after=600.0) -> Vector{LockInfo}

Every `.running` lock among `keys` (or, without `keys`, every one under the vault's status
directory, named by path), with who holds it, how old its heartbeat and its progress are, and the
verdict of [`judge_lock`](@ref). Read-only: nothing is removed. Usable at any time, from any
process that can read the vault.
"""
function locks(vault::Vault, keys::AbstractVector{DataKey}; stale_after::Real=600.0)
    masters = read_status(vault)
    pages = _progress_ages(masters)
    out = LockInfo[]
    for k in keys
        lk = _lock_now(vault, k)
        lk === nothing && continue
        push!(out, _lock_info(canonical(k), lk.owner, lk.age, masters, pages, stale_after))
    end
    return out
end

function locks(vault::Vault; stale_after::Real=600.0)
    dir = joinpath(vault.outdir, "status", vault.spec.study.project_name, vault.run)
    return _locks_under(dir, read_status(vault), stale_after)
end

"""
    locks(outdir::AbstractString; stale_after=600.0) -> Vector{LockInfo}

[`locks`](@ref) for every project and run under an `outdir`, without opening a vault.
"""
function locks(outdir::AbstractString; stale_after::Real=600.0)
    return _locks_under(joinpath(outdir, "status"), read_status(outdir), stale_after)
end

# `owner=` and `heartbeat_unix=` out of a `.running` file, read by path. The key-based entry points
# go through DataVault's accessors; this is for when there is no key to ask with. The two fields
# are the part of the file's format that is already a contract between the packages (the owner
# token is written here and parsed here).
function _read_lock_file(path::AbstractString)
    content = try
        read(path, String)
    catch
        return nothing
    end
    owner = nothing
    hb = nothing
    for line in eachsplit(content, '\n')
        if startswith(line, "owner=")
            owner = String(line[7:end])
        elseif startswith(line, "heartbeat_unix=")
            hb = tryparse(Float64, line[16:end])
        end
    end
    age = hb === nothing ? time() - mtime(path) : time() - hb
    return (owner, max(age, 0.0))
end

function _locks_under(dir::AbstractString, masters, stale_after)
    out = LockInfo[]
    isdir(dir) || return out
    pages = _progress_ages(masters)
    for (root, _, files) in walkdir(dir)
        for f in files
            endswith(f, ".running") || continue
            r = _read_lock_file(joinpath(root, f))
            r === nothing && continue
            name = relpath(joinpath(root, f), dir)
            push!(out, _lock_info(name, r[1], r[2], masters, pages, stale_after))
        end
    end
    return sort!(out; by=l -> l.key)
end

"""
    lock_summary(infos) -> NamedTuple

`(; held, held_jobs, dead, dead_jobs, stale, unknown)` over a list of [`LockInfo`](@ref): how many
locks are held and by how many distinct jobs (a holder with no job id counts by host), how many
are dead and of how many jobs, and how many could not be decided.
"""
function lock_summary(infos::AbstractVector{LockInfo})
    who(l) = isempty(l.job) ? string(l.host, ":", l.pid) : l.job
    held = [l for l in infos if l.verdict === :held]
    dead = [l for l in infos if l.verdict === :dead]
    return (;
        held=length(held),
        held_jobs=length(unique(who.(held))),
        dead=length(dead),
        dead_jobs=length(unique(who.(dead))),
        stale=count(l -> l.verdict === :stale, infos),
        unknown=count(l -> l.verdict === :unknown, infos),
    )
end

function _summary_line(s)
    return "locks: $(s.held) held by $(s.held_jobs) job(s), $(s.dead) dead " *
           "($(s.dead_jobs) dead job(s)), $(s.stale) stale, $(s.unknown) unknown"
end

"""
    print_locks([io], vault_or_outdir; stale_after=600.0)

Print [`locks`](@ref): the summary line, then one line per lock with its verdict, holder,
heartbeat age, progress age and the evidence.
"""
print_locks(x; kwargs...) = print_locks(stdout, x; kwargs...)

function print_locks(io::IO, x; stale_after::Real=600.0)
    infos = locks(x; stale_after=stale_after)
    println(io, _summary_line(lock_summary(infos)))
    for l in infos
        holder = l.owner === nothing ? "(unstamped)" : string(l.host, ":", l.pid)
        job = isempty(l.job) ? "" : " job $(l.job)"
        prog = if l.progress_age === nothing
            ""
        else
            "  progress $(round(Int, l.progress_age)) s ago"
        end
        println(
            io,
            "  ",
            rpad(l.verdict, 8),
            l.key,
            "  ",
            holder,
            job,
            "  heartbeat $(round(Int, l.age)) s ago",
            prog,
            "  (",
            l.why,
            ")",
        )
    end
    return nothing
end

"""
    reap_dead_locks!(vault, keys; stale_after=600.0, log=nothing) -> NamedTuple

Remove every lock among `keys` whose verdict is `:dead`, and return [`lock_summary`](@ref) of what
was found plus `reaped`, how many were actually removed. This is the pass [`run!`](@ref) makes
before it builds its queue; call it yourself to clean a vault without running anything.

Conservative on purpose: only `:dead` is removed, the removal is owner-checked (a lock reclaimed
since it was judged is left alone), and `:stale` / `:unknown` are left to
`DataVault.acquire_running!`.
"""
function reap_dead_locks!(
    vault::Vault,
    keys::AbstractVector{DataKey};
    stale_after::Real=600.0,
    log::Union{EventLog,Nothing}=nothing,
)
    masters = read_status(vault)
    pages = _progress_ages(masters)
    infos = LockInfo[]
    reaped = 0
    for k in keys
        lk = _lock_now(vault, k)
        lk === nothing && continue
        info = _lock_info(canonical(k), lk.owner, lk.age, masters, pages, stale_after)
        push!(infos, info)
        info.verdict === :dead || continue
        _reap!(vault, k, info, Symbol(vault.run), log) && (reaped += 1)
    end
    return (; lock_summary(infos)..., reaped=reaped)
end

"""
    reap_dead_locks!(vault; stale_after=600.0, log=nothing) -> NamedTuple
    reap_dead_locks!(outdir::AbstractString; stale_after=600.0, log=nothing) -> NamedTuple

The same for EVERY `.running` under the vault's status directory (or under a whole `outdir`),
found by path: locks on keys no current run lists are reached too, which the form that takes
`keys` cannot do. Only a lock judged `:dead` — its holder shown to be gone — is removed; `:stale`
and `:unknown` are left for `stale_after` and the next `run!`.

Returns the [`lock_summary`](@ref) of what it found, with `reaped` and `failed`. The removal is
DataVault's owner-checked release (the file is moved aside, checked to be that owner's, then
removed), so a lock someone reclaimed in between is put back.
"""
function reap_dead_locks!(
    vault::Vault; stale_after::Real=600.0, log::Union{EventLog,Nothing}=nothing
)
    dir = joinpath(vault.outdir, "status", vault.spec.study.project_name, vault.run)
    return _reap_under!(dir, read_status(vault), stale_after, log)
end

function reap_dead_locks!(
    outdir::AbstractString; stale_after::Real=600.0, log::Union{EventLog,Nothing}=nothing
)
    return _reap_under!(joinpath(outdir, "status"), read_status(outdir), stale_after, log)
end

function _reap_under!(dir::AbstractString, masters, stale_after, log)
    infos = _locks_under(dir, masters, stale_after)
    reaped = failed = 0
    for info in infos
        (info.verdict === :dead && info.owner !== nothing) || continue
        path = joinpath(dir, info.key)                # `_locks_under` names a lock by its path
        ok = try
            DataVault._release_lock_at!(path, info.owner)
        catch e
            e isa InterruptException && rethrow()
            log === nothing ||
                log_event(log, :reap_failed; lock=info.key, err=_short_err(e))
            failed += 1
            continue
        end
        ok || continue                                # released or reclaimed meanwhile
        reaped += 1
        log === nothing || log_event(
            log,
            :lock_reaped;
            lock=info.key,
            owner=info.owner,
            why=info.why,
            age=round(Int, info.age),
        )
    end
    return (; lock_summary(infos)..., reaped=reaped, failed=failed)
end

# Remove one lock judged `:dead`. Nothing here may be fatal: reaping is an optimisation over
# `stale_after`, and an unlink that fails must not take the round with it.
function _reap!(vault::Vault, key::DataKey, info::LockInfo, stage::Symbol, log)::Bool
    info.owner === nothing && return false
    try
        cleared = DataVault.clear_running!(vault, key, info.owner)
        cleared &&
            log !== nothing &&
            log_event(
                log,
                :lock_reaped;
                stage=stage,
                key=canonical(key),
                owner=info.owner,
                why=info.why,
                age=round(Int, info.age),
            )
        return cleared
    catch e
        e isa InterruptException && rethrow()
        log === nothing ||
            log_event(log, :reap_failed; stage=stage, key=canonical(key), err=_short_err(e))
        return false
    end
end

# Exported: the names that say what they are. The rest of this file's API is documented and used
# qualified (`SweepRunner.locks`): a name that short or that common is not this package's to put in
# a caller's namespace.
export LockInfo, judge_lock, print_locks, reap_dead_locks!
