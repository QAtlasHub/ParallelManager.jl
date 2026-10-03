# Control — a running sweep takes orders.
#
# A sweep used to be fixed when it started. Adding work, stopping part of it, changing the order
# or the number of workers all meant editing files under running jobs, touching flag files, or
# cancelling and resubmitting.
#
# A request is one small JSON file under the sweep's state directory:
#
#     <state_root>/control/requests/<id>.json
#
# Every master on that `(project, run)` polls the directory (`RunOpts.control_interval`), applies
# the requests made since it started, writes an acknowledgement, and logs who asked for what.
# A file, like the status: it works on any cluster, survives a login-node disconnect, and reaches
# every master sharing the vault without a broker — they all read the same directory.
#
#   enqueue     add units to the queue
#   cancel      drop queued units matching a filter (optionally stop the running ones too)
#   stop        running units stop at their next safe point; after `grace` they are cut and their
#               locks released. Scope: everything, a filter, or a node
#   prioritise  move matching units to the front
#   resize      retire workers down to `n`, or start more through the `spawn` hook
#   drain       stop dispatching to a node
#   pause       no new dispatch; running units continue
#   resume      undo pause

using JSON3
using Distributed
using DataVault
using ParamIO
using ParamIO: DataKey, canonical

const _CONTROL_OPS = (
    :enqueue, :cancel, :stop, :prioritise, :resize, :drain, :pause, :resume
)

"""
    control_dir(vault) -> String

`<state_root>/control`: `requests/<id>.json` written by [`control!`](@ref), and
`acks/<id>/<master id>.json` written by each master that applied one.
"""
control_dir(vault::Vault) = joinpath(state_root(vault), "control")

# ── filters ─────────────────────────────────────────────────────────────────────────────────────

_as_vector(x::AbstractVector) = collect(Any, x)
_as_vector(x) = Any[x]

"""
    KeyFilter(; select=nothing, samples=nothing)

A predicate over keys that is DATA, so it can travel in a request: `select` maps a parameter name
(the dotted key, as in `key.params`) to the value or values it may take, `samples` lists the
sample indices. A key matches when every named parameter has one of its values and its sample is
listed; an empty filter matches every key.

```julia
KeyFilter(select = Dict("system.N" => [32, 64], "model.J" => 1.0))
```
"""
function KeyFilter(; select=nothing, samples=nothing)
    w = Dict{String,Vector{Any}}()
    if select !== nothing
        for (k, v) in pairs(select)
            w[String(k)] = _as_vector(v)
        end
    end
    s = samples === nothing ? nothing : Int[Int(x) for x in _as_vector(samples)]
    return KeyFilter(w, s)
end

Base.isempty(f::KeyFilter) = isempty(f.select) && f.samples === nothing

# Numbers compare by value (a request written as `32` matches a parameter held as `32.0`);
# anything else by its printed form, since a request can only carry JSON.
_same(a::Number, b::Number) = a == b
_same(a, b) = string(a) == string(b)

"""
    matches(filter, key) -> Bool
"""
function matches(f::KeyFilter, key::DataKey)
    f.samples === nothing || key.sample in f.samples || return false
    for (name, vals) in f.select
        haskey(key.params, name) || return false
        any(v -> _same(key.params[name], v), vals) || return false
    end
    return true
end

function _filter_of(req::AbstractDict)
    return KeyFilter(;
        select=get(req, "select", nothing), samples=get(req, "samples", nothing)
    )
end

# ── sending a request ───────────────────────────────────────────────────────────────────────────

# A parameter value with its type beside it. JSON alone does not keep `1.0` apart from `1`, and a
# key whose float came back as an integer is a different key (a different path, a different
# canonical string).
function _enc(v)
    v isa Bool && return ["b", string(v)]
    v isa Integer && return ["i", string(v)]
    v isa AbstractFloat && return ["f", repr(Float64(v))]
    v isa AbstractString && return ["s", String(v)]
    return ["?", string(v)]
end

function _dec(t::AbstractString, s::AbstractString)
    t == "b" && return parse(Bool, s)
    t == "i" && return parse(Int, s)
    t == "f" && return parse(Float64, s)
    return String(s)
end

function _key_json(k::DataKey)
    return Dict{String,Any}(
        "params" => Dict{String,Any}(n => _enc(v) for (n, v) in k.params),
        "sample" => k.sample,
    )
end

function _key_from(d::AbstractDict)
    params = Dict{String,Any}(
        String(n) => _dec(String(tv[1]), String(tv[2])) for (n, tv) in pairs(d["params"])
    )
    return DataKey(params, Int(d["sample"]))
end

# The keys a request carries must come out the other side as the SAME keys. The encoding keeps
# what a config's scalars can be (integers, floats, strings, booleans); a key built from anything
# else is refused here, by the sender, rather than turned into a different key on a compute node.
function _check_roundtrip(keys::AbstractVector{DataKey})
    for k in keys
        back = _key_from(JSON3.read(JSON3.write(_key_json(k)), Dict{String,Any}))
        canonical(back) == canonical(k) || throw(
            ArgumentError(
                "control!: key $(canonical(k)) does not survive a JSON round trip " *
                "(it reads back as $(canonical(back))); pass `config=` instead of `keys=`",
            ),
        )
    end
    return nothing
end

function _new_request_id()
    return string(round(Int, time() * 1000), "_", string(rand(UInt32); base=16, pad=8))
end

"""
    control!(vault, op; select=nothing, samples=nothing, keys=nothing, config=nothing,
             running=false, grace=nothing, interrupt=false, node=nothing, n=nothing,
             master=nothing) -> String

Send a request to every master running on `vault`'s `(project, run)` and return its id. `op`:

| `op`          | arguments                         | effect                                              |
| :------------ | :-------------------------------- | :-------------------------------------------------- |
| `:enqueue`    | `keys=` or `config=`              | add units to the queue; `config` is a ParamIO config the master expands. Units already done are settled, not recomputed |
| `:cancel`     | `select=`, `samples=`, `running=`  | drop queued units matching the filter, for the rest of the job; with `running=true` also stop the running ones (see `:stop`) |
| `:stop`       | `select=`/`samples=`, or `node=`, or neither; `grace=` | running units in scope stop at their next safe point ([`should_stop`](@ref)); queued units in scope are not started. With neither filter nor node the scope is everything and the master returns. After `grace` seconds a unit still running is cut: its worker is removed, then its lock released, so the round returns and another worker or job can take the key |
| `:prioritise` | `select=`, `samples=`              | move matching queued units to the front             |
| `:resize`     | `n=`                              | retire workers down to `n` (idle ones first, each after its current unit), or start more through the `spawn` hook of [`run!`](@ref) |
| `:drain`      | `node=`                           | stop dispatching to the workers on a node           |
| `:pause`      |                                   | no new dispatch; running units continue             |
| `:resume`     |                                   | undo `:pause`                                       |

`interrupt` is accepted for compatibility and ignored: a cut now removes the worker.

`master` limits the request to one master (its id, or its scheduler job id). A master applies only
requests made after it started, so an old `:stop` does not stop next week's job.

A request is a file in [`control_dir`](@ref); each master that applies it writes an
acknowledgement ([`read_acks`](@ref)) and a `control_request` event naming who asked and when.
"""
function control!(
    vault::Vault,
    op::Symbol;
    select=nothing,
    samples=nothing,
    keys::Union{AbstractVector{DataKey},Nothing}=nothing,
    config::Union{AbstractString,Nothing}=nothing,
    running::Bool=false,
    grace::Union{Real,Nothing}=nothing,
    interrupt::Bool=false,
    node::Union{AbstractString,Nothing}=nothing,
    n::Union{Integer,Nothing}=nothing,
    master::Union{AbstractString,Nothing}=nothing,
)
    return _write_request(
        control_dir(vault),
        op;
        select,
        samples,
        keys,
        config,
        running,
        grace,
        interrupt,
        node,
        n,
        master,
    )
end

"""
    control!(outdir::AbstractString, op; project=nothing, run=nothing, kwargs...) -> Vector{String}

[`control!`](@ref) without a vault: the request goes to every `(project, run)` under `outdir` that
has SweepRunner state (optionally only one `project` and/or `run`). Returns the ids, one per
`(project, run)` reached.
"""
function control!(
    outdir::AbstractString,
    op::Symbol;
    project::Union{AbstractString,Nothing}=nothing,
    run::Union{AbstractString,Nothing}=nothing,
    kwargs...,
)
    ids = String[]
    base = joinpath(outdir, "sweeprunner")
    isdir(base) || return ids
    for p in readdir(base)
        (project === nothing || p == project) || continue
        isdir(joinpath(base, p)) || continue
        for r in readdir(joinpath(base, p))
            (run === nothing || r == run) || continue
            root = joinpath(base, p, r)
            isdir(root) || continue
            push!(ids, _write_request(joinpath(root, "control"), op; kwargs...))
        end
    end
    return ids
end

function _write_request(
    dir::AbstractString,
    op::Symbol;
    select=nothing,
    samples=nothing,
    keys=nothing,
    config=nothing,
    running::Bool=false,
    grace=nothing,
    interrupt::Bool=false,
    node=nothing,
    n=nothing,
    master=nothing,
)
    op in _CONTROL_OPS ||
        throw(ArgumentError("control!: unknown op $(repr(op)); one of $(_CONTROL_OPS)"))
    if op === :enqueue
        (keys === nothing) == (config === nothing) && throw(
            ArgumentError("control!(:enqueue) takes exactly one of `keys` or `config`")
        )
        keys === nothing || _check_roundtrip(keys)
        config === nothing ||
            isfile(config) ||
            throw(ArgumentError("control!(:enqueue): no config file at $config"))
    elseif op === :resize
        (n !== nothing && n >= 0) ||
            throw(ArgumentError("control!(:resize) needs `n >= 0`"))
    elseif op === :drain
        node === nothing && throw(ArgumentError("control!(:drain) needs `node`"))
    end
    grace === nothing ||
        grace >= 0 ||
        throw(ArgumentError("control!: `grace` must be >= 0, got $grace"))
    flt = KeyFilter(; select, samples)

    id = _new_request_id()
    req = Dict{String,Any}(
        "id" => id,
        "op" => String(op),
        "at" => time(),
        "by" => string(get(ENV, "USER", "?"), "@", gethostname()),
    )
    isempty(flt.select) || (req["select"] = flt.select)
    flt.samples === nothing || (req["samples"] = flt.samples)
    keys === nothing || (req["keys"] = [_key_json(k) for k in keys])
    config === nothing || (req["config"] = abspath(config))
    running && (req["running"] = true)
    interrupt && (req["interrupt"] = true)
    grace === nothing || (req["grace"] = Float64(grace))
    node === nothing || (req["node"] = String(node))
    n === nothing || (req["n"] = Int(n))
    master === nothing || (req["target"] = String(master))
    atomic_write(io -> JSON3.write(io, req), joinpath(dir, "requests", id * ".json"))
    return id
end

"""
    read_requests(vault) -> Vector{Dict{String,Any}}

Every request sent to this `(project, run)`, oldest first.
"""
function read_requests(vault::Vault)
    dir = joinpath(control_dir(vault), "requests")
    out = Dict{String,Any}[]
    isdir(dir) || return out
    for f in sort(readdir(dir))
        endswith(f, ".json") || continue
        r = _read_json(joinpath(dir, f))
        r === nothing || push!(out, r)
    end
    return out
end

"""
    read_acks(vault, id) -> Vector{Dict{String,Any}}

What each master that applied request `id` said it did: `master`, `at`, `op`, and `detail` (for
example how many units a `:cancel` dropped). No entry from a master means it has not seen the
request yet, or the request was not for it.
"""
function read_acks(vault::Vault, id::AbstractString)
    dir = joinpath(control_dir(vault), "acks", id)
    out = Dict{String,Any}[]
    isdir(dir) || return out
    for f in sort(readdir(dir))
        endswith(f, ".json") || continue
        r = _read_json(joinpath(dir, f))
        r === nothing || push!(out, r)
    end
    return out
end

function _read_json(path::AbstractString)
    try
        return JSON3.read(read(path, String), Dict{String,Any})
    catch e
        e isa InterruptException && rethrow()
        return nothing
    end
end

# ── the master's side ───────────────────────────────────────────────────────────────────────────

# Is request `req` addressed to the master `id` running as scheduler job `job`, and recent enough?
function _for_me(req::AbstractDict, since::Float64, id::AbstractString, job::AbstractString)
    Float64(get(req, "at", 0.0)) >= since || return false
    tgt = get(req, "target", nothing)
    return tgt === nothing || tgt == id || (!isempty(job) && tgt == job)
end

"""
    poll_control!(master, table, log, opts; affinity=nothing, force=false) -> Bool

Apply the requests this master has not seen yet, and enforce the stop orders in force (cut what is
past its grace). Returns whether anything changed. Rate-limited to `opts.control_interval` unless
`force`; with `control_interval == 0` the master takes no requests.
"""
function poll_control!(
    m::Master,
    table::Union{TaskTable,Nothing},
    log::EventLog,
    opts::RunOpts;
    affinity=nothing,
    force::Bool=false,
)::Bool
    v = m.vault
    (v === nothing || opts.control_interval <= 0) && return false
    c = m.ctl
    (!force && time() - c.last_poll < opts.control_interval) && return false
    c.last_poll = time()
    changed = false
    dir = joinpath(control_dir(v), "requests")
    if isdir(dir)
        for f in sort(readdir(dir))
            endswith(f, ".json") || continue
            id = f[1:(end - 5)]
            id in c.seen && continue
            # Read, THEN mark seen: a read that fails (a partial read on a network file system)
            # is tried again, and one that keeps failing is said, not dropped.
            req = _read_json(joinpath(dir, f))
            if req === nothing || !haskey(req, "op") || !haskey(req, "id")
                n = c.unread[id] = get(c.unread, id, 0) + 1
                n >= _UNREADABLE_TRIES || continue
                push!(c.seen, id)
                why = "request file could not be read as a request after $n tries"
                log_event(
                    log, :control_bad_request; level=:warn, stage=m.stage, id=id, err=why
                )
                _ack(
                    v,
                    m,
                    Dict{String,Any}("id" => id, "op" => "?"),
                    Dict{String,Any}("error" => why),
                    log,
                )
                continue
            end
            push!(c.seen, id)
            _for_me(req, m.started, m.id, m.job) || continue
            detail = try
                _apply_request!(m, table, req, log, opts, affinity)
            catch e
                e isa InterruptException && rethrow()
                Dict{String,Any}("error" => _short_err(e))
            end
            _ack(v, m, req, detail, log)
            # A request that was not carried out is a warning, not one more line at info.
            failed = haskey(detail, "error") || haskey(detail, "unsupported")
            log_event(
                log,
                failed ? :control_not_applied : :control_request;
                level=failed ? :warn : :info,
                stage=m.stage,
                id=id,
                op=get(req, "op", "?"),
                by=get(req, "by", "?"),
                asked_at=get(req, "at", nothing),
                detail=detail,
            )
            changed = true
        end
    end
    table === nothing || _enforce_stops!(m, table, log) && (changed = true)
    return changed
end

# How many polls a request file may be unreadable before it is given up on, and said.
const _UNREADABLE_TRIES = 3

function _ack(v::Vault, m::Master, req::AbstractDict, detail, log=nothing)
    try
        path = joinpath(control_dir(v), "acks", String(req["id"]), m.id * ".json")
        ack = Dict{String,Any}(
            "master" => m.id,
            "id" => req["id"],
            "op" => get(req, "op", "?"),
            "at" => time(),
            "detail" => detail,
        )
        atomic_write(io -> JSON3.write(io, ack), path)
    catch e
        e isa InterruptException && rethrow()
        # The sender is waiting on this file to know whether anything took the request.
        log === nothing || log_event(
            log,
            :control_ack_failed;
            level=:warn,
            stage=m.stage,
            id=get(req, "id", "?"),
            err=_short_err(e),
        )
    end
    return nothing
end

function _apply_request!(
    m::Master,
    table::Union{TaskTable,Nothing},
    req::AbstractDict,
    log::EventLog,
    opts::RunOpts,
    affinity,
)
    c = m.ctl
    op = Symbol(req["op"])
    d = Dict{String,Any}()
    if op === :pause
        c.paused = true
    elseif op === :resume
        c.paused = false
    elseif op === :enqueue
        ks = if haskey(req, "keys")
            DataKey[_key_from(k) for k in req["keys"]]
        else
            ParamIO.expand(ParamIO.load(String(req["config"])))
        end
        have = Set(canonical(k) for k in c.extra)
        fresh = DataKey[k for k in ks if !(canonical(k) in have)]
        append!(c.extra, fresh)
        d["keys"] = length(ks)
        d["queued"] = table === nothing ? 0 : _enqueue!(m, table, ks, log, opts, affinity)
    elseif op === :cancel
        flt = _filter_of(req)
        push!(c.cancels, flt)
        d["cancelled"] = table === nothing ? 0 : _cancel_queued!(table, flt)
        if get(req, "running", false)
            d["stopping"] = _order_stops!(m, table, r -> matches(flt, r.key), req)
        end
    elseif op === :prioritise
        flt = _filter_of(req)
        push!(c.priorities, flt)
        d["moved"] = table === nothing ? 0 : _prioritise!(table, flt)
    elseif op === :stop
        flt = _filter_of(req)
        node = get(req, "node", nothing)
        if node !== nothing
            push!(c.drained, String(node))
            who = lock(() -> copy(m.who), m.lock)
            on_node = r -> haskey(who, r.worker) && who[r.worker].host == node
            d["stopping"] = _order_stops!(m, table, on_node, req)
        elseif !isempty(flt)
            push!(c.cancels, flt)
            d["cancelled"] = table === nothing ? 0 : _cancel_queued!(table, flt)
            d["stopping"] = _order_stops!(m, table, r -> matches(flt, r.key), req)
        else
            c.stop_all = true
            d["stopping"] = _order_stops!(m, table, r -> true, req)
        end
    elseif op === :drain
        push!(c.drained, String(req["node"]))
    elseif op === :resize
        c.target = Int(req["n"])
        merge!(d, _resize!(m, table, log))
    else
        d["error"] = "unknown op"
    end
    return d
end

# Add keys to a table that is being drawn. A key that is done is settled, one that is locked is
# held: the same reading `_scan!` gives the keys the round started with.
function _enqueue!(m::Master, table::TaskTable, ks, log::EventLog, opts::RunOpts, affinity)
    before = length(table)
    add_tasks!(table, ks; affinity=m.multi ? affinity : nothing)
    v = m.vault
    progress = read_progress(v)
    masters = _lazy_masters(v)
    infos = LockInfo[]
    stage = Symbol(m.stage)
    queued = 0
    for i in (before + 1):length(table)
        s = _scan_row!(table, i, v, stage, log, opts, progress, masters, infos)
        (s === :free || s === :reaped || s === :stale) && (queued += 1)
    end
    _apply_standing!(m, table)
    return queued
end

# Rows that are waiting (queued, or held by a sibling) and match: this job is finished with them.
function _cancel_queued!(table::TaskTable, flt::KeyFilter)
    idx = lock(table.lock) do
        return [
            i for (i, r) in enumerate(table.rows) if
            (r.state === :todo || r.state === :held) && matches(flt, r.key)
        ]
    end
    foreach(i -> settle!(table, i, :cancelled), idx)
    return length(idx)
end

function _prioritise!(table::TaskTable, flt::KeyFilter)
    idx = lock(table.lock) do
        return [
            i for
            (i, r) in enumerate(table.rows) if r.state === :todo && matches(flt, r.key)
        ]
    end
    foreach(i -> requeue!(table, i; front=true), idx)
    return length(idx)
end

# What a request changed for the rest of the job is applied again to every table the master
# builds: a study cancelled in round 3 is not dispatched in round 4.
function _apply_standing!(m::Master, table::TaskTable)
    for flt in m.ctl.cancels
        _cancel_queued!(table, flt)
    end
    for flt in m.ctl.priorities
        _prioritise!(table, flt)
    end
    return nothing
end

# Record a stop order for every running row `pred` selects. The workers see the request
# themselves (`should_stop`); the order is what lets the master cut a unit that outlives `grace`.
function _order_stops!(m::Master, table::Union{TaskTable,Nothing}, pred, req::AbstractDict)
    table === nothing && return 0
    grace = get(req, "grace", nothing)
    deadline = grace === nothing ? Inf : time() + Float64(grace)
    rows = lock(table.lock) do
        return [r for r in table.rows if r.state === :running && pred(r)]
    end
    for r in rows
        m.ctl.stopping[r.kstr] = StopOrder(
            deadline, false, String(req["id"]), get(req, "interrupt", false) === true
        )
    end
    return length(rows)
end

# Cut the units that were told to stop and are still running past their grace (`_cut!`).
function _enforce_stops!(m::Master, table::TaskTable, log::EventLog)::Bool
    c = m.ctl
    isempty(c.stopping) && return false
    v = m.vault
    changed = false
    for (kstr, o) in collect(c.stopping)
        i = get(table.index, kstr, nothing)
        row = i === nothing ? nothing : table.rows[i]
        if row === nothing || row.state !== :running
            delete!(c.stopping, kstr)
            continue
        end
        (o.cut || time() <= o.deadline) && continue
        o.cut = true
        changed = true
        _cut!(m, row, o, log)
    end
    return changed
end

# Cut one unit that outlived its grace. The worker goes FIRST, then the lock: released while the
# worker was still computing, the key would be taken by another master and the first worker
# would go on writing its checkpoint and progress over the new owner's. With the worker removed
# the dispatch task's call returns, so the round — and the allocation — is not held by a unit
# that was told to stop.
#
# A worker whose launcher this process cannot kill (one started by a cluster manager that keeps
# no process handle) is asked to leave and may not; that is said in the event
# (`worker_removed=false`), and the unit's writes are refused by the owner check instead.
function _cut!(m::Master, row::TaskRow, o::StopOrder, log::EventLog)
    v = m.vault
    tok = row.owner
    pid = row.worker
    task = @async begin
        removed = false
        if pid != 0 && pid != myid()
            _kill_worker!(pid)
            removed = !(pid in procs())
        end
        released = false
        err = nothing
        try
            released = tok === nothing ? false : DataVault.clear_running!(v, row.key, tok)
        catch e
            e isa InterruptException && rethrow()
            err = _short_err(e)
        end
        # The dispatch task releases a dead worker's lock too, and may have got there first:
        # what matters, and what is reported, is that the lock is no longer this unit's.
        if !released && err === nothing
            released = try
                DataVault.running_owner(v, row.key) != tok
            catch
                false
            end
        end
        log_event(
            log,
            :key_cut;
            level=:warn,
            stage=m.stage,
            key=row.kstr,
            owner=tok,
            worker=pid,
            request=o.request,
            worker_removed=removed,
            lock_released=released,
            err=err,
        )
    end
    push!(m.ctl.cuts, task)
    return nothing
end

# Give every running unit a stop order with `grace`, for a stop that did not come as a request
# (the job's flag or deadline).
function _order_stops_all!(m::Master, table::TaskTable, grace::Real, why::AbstractString)
    isfinite(grace) || return 0
    rows = lock(table.lock) do
        return [r for r in table.rows if r.state === :running]
    end
    n = 0
    for r in rows
        haskey(m.ctl.stopping, r.kstr) && continue
        m.ctl.stopping[r.kstr] = StopOrder(time() + grace, false, String(why), false)
        n += 1
    end
    return n
end

# Bring the number of dispatching workers to `ctl.target`.
function _resize!(m::Master, table::Union{TaskTable,Nothing}, log::EventLog)
    c = m.ctl
    d = Dict{String,Any}()
    target = c.target
    (target === nothing || !m.multi) && return (d["unsupported"]="no worker pool"; d)
    who = lock(() -> copy(m.who), m.lock)
    active = [
        p for
        p in workers() if !(p in c.retired) && !(haskey(who, p) && who[p].host in c.drained)
    ]
    if length(active) > target
        busy = Set{Int}()
        table === nothing || lock(table.lock) do
            for r in table.rows
                r.state === :running && push!(busy, r.worker)
            end
        end
        # Idle workers go first, then the most recently added.
        order = sort(active; by=p -> (p in busy, -p))
        gone = order[1:(length(active) - target)]
        union!(c.retired, gone)
        d["retiring"] = length(gone)
    elseif length(active) < target
        want = target - length(active)
        if c.spawn === nothing
            d["unsupported"] = "run! was given no `spawn` hook; cannot start $want worker(s)"
        else
            d["spawning"] = want
            spawn = c.spawn
            @async try
                spawn(want)
            catch e
                e isa InterruptException && rethrow()
                log_event(
                    log,
                    :spawn_failed;
                    level=:warn,
                    stage=m.stage,
                    n=want,
                    err=_short_err(e),
                )
            end
        end
    end
    return d
end

# ── the worker's side ───────────────────────────────────────────────────────────────────────────

"""
    StopRequested()

What [`stop_point`](@ref) throws. `run!` takes it as "this unit stopped where it was told to":
no attempt is spent, the lock is released, and the key is counted with `stop`.
"""
struct StopRequested <: Exception end

function Base.showerror(io::IO, ::StopRequested)
    return print(io, "StopRequested: the unit was asked to stop")
end

# Does a request written since the master started tell THIS unit to stop? Reads only the request
# files not seen before.
function _stop_requested!(w::StopWatch, vault::Vault, key::DataKey)::Bool
    dir = joinpath(control_dir(vault), "requests")
    isdir(dir) || return false
    for f in readdir(dir)
        endswith(f, ".json") || continue
        f in w.seen && continue
        req = _read_json(joinpath(dir, f))
        if req === nothing
            # Tried again at the next look: a stop whose file could not be read once must not
            # be a stop this unit never hears of.
            n = w.unread[f] = get(w.unread, f, 0) + 1
            n >= _UNREADABLE_TRIES && push!(w.seen, f)
            continue
        end
        push!(w.seen, f)
        _for_me(req, w.since, w.master, w.job) || continue
        op = get(req, "op", "")
        flt = _filter_of(req)
        if op == "stop"
            node = get(req, "node", nothing)
            if node !== nothing
                node == gethostname() && return true
            elseif matches(flt, key)                  # an empty filter is "everything"
                return true
            end
        elseif op == "cancel" && get(req, "running", false) === true
            matches(flt, key) && return true
        end
    end
    return false
end

"""
    should_stop(; poll=10.0) -> Bool

Ask, from inside `work_fn`, whether this unit has been told to stop: the job's `stop_flag` was
raised, its `deadline` passed, or a [`control!`](@ref) `:stop` (or `:cancel` with `running=true`)
covers this key or this node. Call it at the points where the unit can leave cleanly — after a
checkpoint, between segments.

This is what gives a stop a bound: without it, a key already in `work_fn` runs to completion, so
the time between raising a flag and `run!` returning is the longest key.

The filesystem is consulted at most once every `poll` seconds; in between, the answer is the last
one. Once `true`, it stays `true`. Outside a `run!` it is `false`.
"""
function should_stop(; poll::Real=10.0)::Bool
    ctx = _KEY[]
    ctx === nothing && return false
    w = ctx.watch
    w.hit && return true
    now = time()
    now - w.checked < poll && return false
    w.checked = now
    hit = try
        _stop_reason(ctx.opts) !== nothing || _stop_requested!(w, ctx.vault, ctx.key)
    catch e
        e isa InterruptException && rethrow()
        false
    end
    hit && (w.hit = true)
    return hit
end

"""
    stop_point(; poll=10.0)

`should_stop() && throw(StopRequested())`: a safe point. Put it where the unit's state on disk is
consistent (after [`report_progress`](@ref)).

```julia
for seg in first:nseg
    run_segment!(key, seg)
    SweepRunner.report_progress(seg; of=nseg)
    SweepRunner.stop_point()
end
```
"""
function stop_point(; poll::Real=10.0)
    should_stop(; poll=poll) && throw(StopRequested())
    return nothing
end

# Exported: the names that say what they are. The rest of this file's API is documented and used
# qualified (`SweepRunner.matches`): a name that short or that common is not this package's to put in
# a caller's namespace.
"""
    wait_acks(vault, id; timeout=30.0, poll=0.5) -> Vector{Dict{String,Any}}

Wait until at least one master has acknowledged request `id`, up to `timeout` seconds, and return
the acknowledgements ([`read_acks`](@ref)). Empty means nobody took it in that time: no master is
running on this `(project, run)`, or none has polled yet.
"""
function wait_acks(vault::Vault, id::AbstractString; timeout::Real=30.0, poll::Real=0.5)
    t0 = time()
    while true
        acks = read_acks(vault, id)
        (!isempty(acks) || time() - t0 >= timeout) && return acks
        sleep(poll)
    end
end

"""
    masters_listening(vault) -> Vector{String}
    masters_listening(outdir::AbstractString; project=nothing, run=nothing) -> Vector{String}

The ids of the masters that will read a request sent now: those whose status says they are
running or between rounds and that have reported recently. A master applies only requests made
after it started, so a request sent when this is empty is applied by nobody — not by the next job
either.
"""
function masters_listening(statuses::AbstractVector)
    return String[
        d["master"] for d in statuses if !d["stale"] && d["state"] in ("running", "waiting")
    ]
end

masters_listening(vault::Vault) = masters_listening(read_status(vault))

function masters_listening(outdir::AbstractString; project=nothing, run=nothing)
    all = read_status(outdir)
    keep =
        d -> begin
            parts = splitpath(d["path"])
            # …/sweeprunner/<project>/<run>/masters/<id>/status.json
            (project === nothing || parts[end - 4] == project) &&
                (run === nothing || parts[end - 3] == run)
        end
    return masters_listening(filter(keep, all))
end

export control!, read_requests, read_acks, wait_acks, masters_listening
export KeyFilter, should_stop, stop_point, StopRequested
