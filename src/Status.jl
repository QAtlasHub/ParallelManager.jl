# Status — what a running sweep is doing, written where it can be asked from outside.
#
# A master knew how many workers it reserved and how many joined. Nothing knew what each worker
# was doing or how busy it was, so "is this 72-node job using its nodes?" meant joining
# `squeue -s`, `scontrol show node` and a grep over the master's log by hand, and a ramp-up that
# stopped at half the planned workers said nothing for the rest of the job.
#
# Each master rewrites one JSON file, atomically, every `RunOpts.status_interval` seconds:
#
#     <state_root>/masters/<master id>/status.json
#
# A file, not a socket: it works on any cluster, needs no extra process and no open port, and is
# still there after the job has ended. `read_status` / `print_status` (and `sweeprunner status`)
# read every master's file under a vault or an outdir.

using Distributed
using JSON3
using LinearAlgebra: BLAS

# ── what the spawner means to have ──────────────────────────────────────────────────────────────

# (planned, launched, when). Process-global because the code that sizes and starts the workers is
# usually not the code that calls `run!`, and may not hold a `Master`.
const _SPAWN = Ref{Tuple{Int,Int,Float64}}((0, 0, 0.0))
const _SPAWN_LOCK = ReentrantLock()

"""
    note_workers!(; planned=nothing, launched=nothing)

Tell the status what the worker pool is SUPPOSED to look like: `planned` workers intended,
`launched` actually started (processes or job steps that exist). The master already knows how many
joined. With these three numbers a ramp-up that stalls is one line of the status, and a
`workers_short` warning in the event log, instead of silence.

Call it from whatever starts the workers, as often as the numbers change. [`init_workers!`](@ref)
calls it for the pools it starts.
"""
function note_workers!(;
    planned::Union{Integer,Nothing}=nothing, launched::Union{Integer,Nothing}=nothing
)
    lock(_SPAWN_LOCK) do
        p, l, _ = _SPAWN[]
        return _SPAWN[] = (
            planned === nothing ? p : Int(planned),
            launched === nothing ? l : Int(launched),
            time(),
        )
    end
    return nothing
end

# ── what a worker says about itself ─────────────────────────────────────────────────────────────

# The cores this process was given. `SLURM_CPUS_PER_TASK` is what a job step was started with;
# without it, the threads it will actually use.
function _my_cores()::Int
    n = tryparse(Int, get(ENV, "SLURM_CPUS_PER_TASK", ""))
    n !== nothing && n > 0 && return n
    return max(Threads.nthreads(), BLAS.get_num_threads())
end

# CPU seconds this process has used (user + system, all threads), or NaN.
function _cpu_seconds()::Float64
    buf = zeros(UInt8, 256)                       # uv_rusage_t is 144 bytes
    rc = ccall(:uv_getrusage, Cint, (Ptr{UInt8},), buf)
    rc == 0 || return NaN
    t = reinterpret(Clong, buf[1:(4 * sizeof(Clong))])   # ru_utime, ru_stime as (sec, usec)
    return t[1] + t[2] / 1e6 + t[3] + t[4] / 1e6
end

function _rss_bytes()::Int
    r = Ref{Csize_t}(0)
    rc = ccall(:uv_resident_set_memory, Cint, (Ref{Csize_t},), r)
    return rc == 0 ? Int(r[]) : 0
end

# One reading. Utilisation is the difference of two of these, taken by the master.
_sample() = (; cpu=_cpu_seconds(), wall=time(), rss=_rss_bytes())

# ── the snapshot ────────────────────────────────────────────────────────────────────────────────

"""
    status_path(vault, master) -> String

`<state_root>/masters/<master id>/status.json`.
"""
function status_path(vault::Vault, m::Master)
    return joinpath(state_root(vault), "masters", m.id, "status.json")
end

# `128(x72)`, `64(x2),32` → total cores of the allocation, or 0 when it cannot be read.
function _slurm_alloc_cores()::Int
    s = get(ENV, "SLURM_JOB_CPUS_PER_NODE", "")
    isempty(s) && return 0
    total = 0
    for part in split(s, ',')
        m = match(r"^(\d+)(?:\(x(\d+)\))?$", strip(part))
        m === nothing && return 0
        total += parse(Int, m[1]) * (m[2] === nothing ? 1 : parse(Int, m[2]))
    end
    return total
end

"""
    expand_nodelist(s) -> Vector{String}

Expand a Slurm node list (`c[001-003,007],gpu01`) into host names. Covers the bracketed range
form `SLURM_JOB_NODELIST` uses; a list it cannot read gives an empty vector rather than a guess.
"""
function expand_nodelist(s::AbstractString)::Vector{String}
    out = String[]
    depth = 0
    parts = String[]
    cur = IOBuffer()
    for c in s
        c == '[' && (depth += 1)
        c == ']' && (depth -= 1)
        if c == ',' && depth == 0
            push!(parts, String(take!(cur)))
        else
            write(cur, c)
        end
    end
    push!(parts, String(take!(cur)))
    for p in parts
        p = strip(p)
        isempty(p) && continue
        m = match(r"^([^\[\]]*)\[([^\[\]]+)\]([^\[\]]*)$", p)
        if m === nothing
            occursin('[', p) && return String[]
            push!(out, String(p))
            continue
        end
        for r in split(m[2], ',')
            ab = split(r, '-')
            if length(ab) == 1
                push!(out, string(m[1], ab[1], m[3]))
            elseif length(ab) == 2
                a, b = tryparse(Int, ab[1]), tryparse(Int, ab[2])
                (a === nothing || b === nothing) && return String[]
                w = length(ab[1])
                for i in a:b
                    push!(out, string(m[1], lpad(i, w, '0'), m[3]))
                end
            else
                return String[]
            end
        end
    end
    return out
end

"""
    status_snapshot(master) -> Dict{String,Any}

What [`write_status`](@ref) writes: the task counts, the worker pool (planned / launched / joined
/ busy / idle, cores busy against cores allocated), one line per node, one row per worker (the key
it is on, since when, the lock it holds, the progress last reported for that key, CPU utilisation
and RSS), and the warnings in force.
"""
function status_snapshot(m::Master)
    now = time()
    table = m.table
    counts = if table === nothing
        (; total=0, todo=0, running=0, held=0, done=0, failed=0, other=0)
    else
        task_counts(table)
    end
    # worker id => the row it is on
    on = Dict{Int,TaskRow}()
    if table !== nothing
        lock(table.lock) do
            for r in table.rows
                r.state === :running && r.worker != 0 && (on[r.worker] = r)
            end
        end
    end
    who, samples, progress = lock(m.lock) do
        return copy(m.who), copy(m.samples), copy(m.progress)
    end
    joined = filter(p -> haskey(who, p), m.multi ? workers() : [myid()])

    rows = Dict{String,Any}[]
    nodes = Dict{String,Dict{String,Any}}()
    busy = 0
    cores_busy = 0
    cores_joined = 0
    for p in sort(joined)
        w = who[p]
        row = get(on, p, nothing)
        s = get(samples, p, nothing)
        d = Dict{String,Any}(
            "id" => p, "host" => w.host, "pid" => w.pid, "cores" => w.cores
        )
        cores_joined += w.cores
        if row !== nothing
            busy += 1
            cores_busy += w.cores
            d["key"] = row.kstr
            d["owner"] = row.owner
            d["since"] = row.since
            pr = get(progress, row.kstr, row.progress)
            if pr !== nothing
                d["progress_step"] = pr.step
                d["progress_of"] = pr.of
                d["progress_at"] = pr.at
            end
        end
        if s !== nothing
            d["cpu"] = isnan(s.util) ? nothing : round(s.util; digits=3)
            d["rss"] = s.rss
            d["sampled"] = s.wall
        end
        push!(rows, d)
        n = get!(nodes, w.host) do
            return Dict{String,Any}(
                "host" => w.host,
                "workers" => 0,
                "busy" => 0,
                "cores" => 0,
                "_u" => Float64[],
            )
        end
        n["workers"] += 1
        n["cores"] += w.cores
        row === nothing || (n["busy"] += 1)
        (s !== nothing && !isnan(s.util)) && push!(n["_u"], s.util)
    end
    nodelist = sort!(collect(values(nodes)); by=n -> n["host"])
    for n in nodelist
        u = pop!(n, "_u")
        n["cpu"] = isempty(u) ? nothing : round(sum(u) / length(u); digits=3)
    end
    allocated = expand_nodelist(get(ENV, "SLURM_JOB_NODELIST", ""))
    empty_nodes = [h for h in allocated if !haskey(nodes, h)]

    planned, launched, _ = lock(() -> _SPAWN[], _SPAWN_LOCK)
    alloc_cores = _slurm_alloc_cores()
    return Dict{String,Any}(
        "master" => m.id,
        "host" => m.host,
        "pid" => m.pid,
        "job" => m.job,
        "stage" => m.stage,
        "state" => String(m.state),
        "started" => m.started,
        "updated" => now,
        "interval" => m.interval,
        "tasks" => Dict{String,Any}(String(k) => v for (k, v) in pairs(counts)),
        "workers" => Dict{String,Any}(
            "planned" => max(planned, m.multi ? 0 : 1),
            "launched" => launched,
            "joined" => length(joined),
            "busy" => busy,
            "idle" => length(joined) - busy,
            "cores_busy" => cores_busy,
            "cores_joined" => cores_joined,
            "cores_allocated" => alloc_cores == 0 ? cores_joined : alloc_cores,
        ),
        "nodes" => nodelist,
        "nodes_without_workers" => empty_nodes,
        "worker_table" => rows,
        "warnings" => copy(m.warnings),
    )
end

"""
    write_status(master) -> Union{String,Nothing}

Write [`status_snapshot`](@ref) to [`status_path`](@ref), atomically, and return the path. A
master that is not attached to a vault yet writes nothing. A write that fails is swallowed: the
status is a view of the run, and must never be what stops it.
"""
function write_status(m::Master)
    v = m.vault
    v === nothing && return nothing
    try
        snap = status_snapshot(m)
        path = status_path(v, m)
        atomic_write(io -> JSON3.write(io, snap), path)
        m.last_status = time()
        return path
    catch e
        e isa InterruptException && rethrow()
        return nothing
    end
end

# ── keeping it fresh ────────────────────────────────────────────────────────────────────────────

# Ask every worker for a reading, without waiting for any of them. A worker inside a `work_fn` that
# does not yield answers when it next does; one question per worker is outstanding at a time.
function _probe_workers!(m::Master)
    if !m.multi
        _store_sample!(m, myid(), _sample())
        return nothing
    end
    for p in workers()
        go = lock(m.lock) do
            (p in m.probing || !haskey(m.who, p)) && return false
            push!(m.probing, p)
            return true
        end
        go || continue
        @async try
            _store_sample!(m, p, remotecall_fetch(_sample, p))
        catch e
            e isa InterruptException && rethrow()
        finally
            lock(() -> delete!(m.probing, p), m.lock)
        end
    end
    return nothing
end

function _store_sample!(m::Master, p::Int, s)
    lock(m.lock) do
        prev = get(m.samples, p, nothing)
        cores = haskey(m.who, p) ? m.who[p].cores : 1
        util = if prev === nothing || s.wall <= prev.wall || isnan(s.cpu) || isnan(prev.cpu)
            NaN
        else
            (s.cpu - prev.cpu) / (s.wall - prev.wall) / max(cores, 1)
        end
        m.samples[p] = WorkerSample(s.cpu, s.wall, s.rss, util)
        return nothing
    end
    return nothing
end

# How long `planned > joined` may last before it is said out loud. Workers that have not joined
# within the worker timeout are not going to.
function _short_after()::Float64
    t = tryparse(Float64, get(ENV, "JULIA_WORKER_TIMEOUT", ""))
    return t === nothing ? 60.0 : t
end

# Warn, once per distinct shortfall, when fewer workers joined than were planned and that has
# lasted longer than the worker timeout.
function _check_short!(m::Master, log::EventLog)
    planned, launched, _ = lock(() -> _SPAWN[], _SPAWN_LOCK)
    joined = m.multi ? nworkers() : 1
    filter!(w -> !startswith(w, "workers_short"), m.warnings)
    if !m.multi || planned <= joined
        m.short_since = 0.0
        return nothing
    end
    m.short_since == 0.0 && (m.short_since = time())
    lasted = time() - m.short_since
    lasted >= _short_after() || return nothing
    push!(
        m.warnings,
        "workers_short: planned $planned, launched $launched, joined $joined " *
        "for $(round(Int, lasted)) s",
    )
    if m.short_logged != (planned, launched, joined)
        m.short_logged = (planned, launched, joined)
        log_event(
            log,
            :workers_short;
            level=:warn,
            stage=m.stage,
            planned=planned,
            launched=launched,
            joined=joined,
            secs=round(Int, lasted),
        )
    end
    return nothing
end

"""
    status_tick!(master, log)

One refresh: take CPU / RSS readings, re-read the progress stamps, check the worker pool against
what was planned, and rewrite the status file. `run!` calls it on a timer
(`RunOpts.status_interval`), and between keys on the sequential path.
"""
function status_tick!(m::Master, log::EventLog)
    try
        _probe_workers!(m)
        v = m.vault
        if v !== nothing
            p = read_progress(v)
            lock(() -> (empty!(m.progress); merge!(m.progress, p)), m.lock)
        end
        _check_short!(m, log)
    catch e
        e isa InterruptException && rethrow()
    end
    return write_status(m)
end

# Rate-limited `status_tick!` for code that has no timer (the sequential path, between keys).
function _status_due!(m::Master, log::EventLog)
    (m.interval > 0 && time() - m.last_status >= m.interval) || return nothing
    status_tick!(m, log)
    return nothing
end

# Run `f()` with the status refreshed every `master.interval` seconds, and once more when it ends.
function _with_status(f, m::Master, log::EventLog)
    m.interval > 0 || return f()
    status_tick!(m, log)
    timer = Timer(m.interval; interval=m.interval) do _
        return status_tick!(m, log)
    end
    try
        return f()
    finally
        close(timer)
        status_tick!(m, log)
    end
end

# ── asking from outside ─────────────────────────────────────────────────────────────────────────

# `<outdir>/sweeprunner/<project>/<run>/masters/<id>/status.json`, found without walking the data.
function _status_files(outdir::AbstractString)
    acc = String[]
    base = joinpath(outdir, "sweeprunner")
    isdir(base) || return acc
    for project in readdir(base; join=true)
        isdir(project) || continue
        for run in readdir(project; join=true)
            masters = joinpath(run, "masters")
            isdir(masters) || continue
            for mdir in readdir(masters; join=true)
                f = joinpath(mdir, "status.json")
                isfile(f) && push!(acc, f)
            end
        end
    end
    return acc
end

function _vault_status_files(vault::Vault)
    masters = joinpath(state_root(vault), "masters")
    isdir(masters) || return String[]
    return [
        joinpath(d, "status.json") for
        d in readdir(masters; join=true) if isfile(joinpath(d, "status.json"))
    ]
end

function _read_status_files(files)
    out = Dict{String,Any}[]
    for f in files
        try
            d = JSON3.read(read(f, String), Dict{String,Any})
            age = time() - Float64(d["updated"])
            d["path"] = f
            d["age"] = age
            # A master that stopped rewriting its file without saying it ended is gone (killed at
            # the wall clock, or the node was lost).
            d["stale"] = d["state"] != "ended" && age > 3 * max(Float64(d["interval"]), 1.0)
            push!(out, d)
        catch e
            e isa InterruptException && rethrow()
        end
    end
    return sort!(out; by=d -> (d["stage"], d["started"]))
end

"""
    read_status(vault) -> Vector{Dict{String,Any}}
    read_status(outdir::AbstractString) -> Vector{Dict{String,Any}}

Every master's last status under a vault's `(project, run)`, or under a whole `outdir` (every
project and run in it). Each entry is what [`status_snapshot`](@ref) wrote, plus `path`, `age`
(seconds since it was written) and `stale`: the master has not rewritten its file for three
intervals and did not say it ended.

Works from any process that can read the directory, while the job runs or after it.
"""
read_status(vault::Vault) = _read_status_files(_vault_status_files(vault))
read_status(outdir::AbstractString) = _read_status_files(_status_files(outdir))

_pct(a, b) = b == 0 ? "-" : string(round(Int, 100 * a / b), "%")

"""
    print_status([io], vault_or_outdir; workers=false)

Print [`read_status`](@ref): per master, the task counts, the worker pool against what was
planned, cores busy against cores allocated, the nodes with no worker, the warnings, and one line
per node. `workers=true` adds one line per worker.
"""
print_status(x; kwargs...) = print_status(stdout, x; kwargs...)

function print_status(io::IO, x; workers::Bool=false)
    all = read_status(x)
    if isempty(all)
        println(io, "no status found")
        return nothing
    end
    for d in all
        state = d["stale"] ? "GONE (no update)" : d["state"]
        job = isempty(d["job"]) ? "" : " job $(d["job"])"
        age = round(Int, d["age"])
        println(io, d["stage"], "  ", d["master"], job, "  ", state, "  updated $age s ago")
        t = d["tasks"]
        println(
            io,
            "  tasks    total $(t["total"])  done $(t["done"])  running $(t["running"])  ",
            "todo $(t["todo"])  held $(t["held"])  failed $(t["failed"])",
        )
        w = d["workers"]
        println(
            io,
            "  workers  planned $(w["planned"])  launched $(w["launched"])  ",
            "joined $(w["joined"])  busy $(w["busy"])  idle $(w["idle"])",
        )
        println(
            io,
            "  cores    busy $(w["cores_busy"]) of $(w["cores_allocated"]) allocated ",
            "($(_pct(w["cores_busy"], w["cores_allocated"])))",
        )
        none = d["nodes_without_workers"]
        isempty(none) || println(
            io, "  nodes    $(length(none)) allocated with no worker: ", join(none, " ")
        )
        for msg in d["warnings"]
            println(io, "  ! ", msg)
        end
        if !isempty(d["nodes"])
            println(io, "  node                 workers  busy  cores   cpu")
            for n in d["nodes"]
                cpu = n["cpu"] === nothing ? "-" : string(round(n["cpu"]; digits=2))
                println(
                    io,
                    "  ",
                    rpad(n["host"], 20),
                    lpad(n["workers"], 8),
                    lpad(n["busy"], 6),
                    lpad(n["cores"], 7),
                    lpad(cpu, 6),
                )
            end
        end
        if workers
            println(io, "  worker  host                 cores   cpu  on key for   key")
            for r in d["worker_table"]
                cpu = get(r, "cpu", nothing)
                key = get(r, "key", nothing)
                secs = if key === nothing
                    ""
                else
                    string(round(Int, d["updated"] - r["since"]), " s")
                end
                println(
                    io,
                    "  ",
                    lpad(r["id"], 6),
                    "  ",
                    rpad(r["host"], 20),
                    lpad(r["cores"], 6),
                    lpad(cpu === nothing ? "-" : string(round(cpu; digits=2)), 6),
                    lpad(secs, 11),
                    "   ",
                    key === nothing ? "(idle)" : key,
                )
            end
        end
    end
    return nothing
end

export note_workers!, WorkerSample, status_path, status_snapshot, write_status
export status_tick!, read_status, print_status, expand_nodelist
