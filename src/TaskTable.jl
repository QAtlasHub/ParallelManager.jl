# TaskTable — the state of the work, held by the master.
#
# Before this, nobody held it. `run!` built a list of keys, `pmap` handed them out flat, and each
# worker found out for itself whether a key was done, locked, or half finished. The master could
# not say what was left, what was running or who had it, and a key held by another job was
# dispatched anyway so the worker could discover that.
#
# The table is read from disk ONCE per round, by the master, and is what the dispatcher draws
# from. The markers stay the durable record and the cross-job lock (several masters share one
# vault, and a master can be killed at the wall clock), so this is the in-memory view of them, not
# a replacement.

using ParamIO: DataKey, canonical

"""
    Progress(step, of, at, note)

How far a unit got: the last `step` finished, out of `of` when the total is known, stamped at
`at` (`time()`). Written by [`report_progress`](@ref) inside `work_fn`, read back by
[`resume_point`](@ref) on the next attempt.
"""
struct Progress
    step::Int
    of::Union{Int,Nothing}
    at::Float64
    note::String
end

"""
    TaskRow

One unit of a sweep in a [`TaskTable`](@ref).

- `state` — `:todo` (queued), `:running` (handed to `worker`), `:held` (locked by another master,
  not queued), `:settled` (this round is finished with it; see `outcome`).
- `outcome` — set when settled, one of `SweepRunner.OUTCOMES`: `:ok`, `:already_done`,
  `:lock_busy`, `:deferred`, `:worker_lost`, `:error`, `:gave_up`, `:stop_flag`, `:stop_deadline`,
  `:stop_request`, `:stopped`, `:cancelled`, `:no_fit` (not started: it could not get anywhere
  before the deadline).
- `owner` — the lock token: ours while `:running`, the holder's while `:held`.
- `worker` — the Distributed id it was handed to (`0` when none).
- `since` — `time()` at which it entered its current state.
- `progress` — the last [`Progress`](@ref) recorded for the key, by any job.
- `deaths` — workers that exited while holding it.
"""
mutable struct TaskRow
    const key::DataKey
    const kstr::String
    const group::Any
    state::Symbol
    outcome::Union{Symbol,Nothing}
    owner::Union{String,Nothing}
    worker::Int
    since::Float64
    progress::Union{Progress,Nothing}
    deaths::Int
end

"""
    TaskTable(keys; affinity=nothing)

The master's table of a round's units, in the caller's order, and the queue the dispatcher draws
from. `affinity` is `key -> group`, as in [`run!`](@ref).

Every method takes the table's lock, so the per-worker dispatch tasks and a status writer can
share one table.
"""
struct TaskTable
    rows::Vector{TaskRow}
    index::Dict{String,Int}
    # group => row indices still queued, REVERSED so `pop!` hands them out in the caller's order.
    # That order is load-bearing: a leading paramset is how a long acquisition is told which slice
    # to close first.
    pending::Dict{Any,Vector{Int}}
    # Rows that go before any group preference (a prioritise request), in order.
    urgent::Vector{Int}
    # worker => the groups it has handled, most recent first.
    seen::Dict{Int,Vector{Any}}
    lock::ReentrantLock
end

function TaskTable(keys::AbstractVector{DataKey}; affinity=nothing)
    t = TaskTable(
        TaskRow[],
        Dict{String,Int}(),
        Dict{Any,Vector{Int}}(),
        Int[],
        Dict{Int,Vector{Any}}(),
        ReentrantLock(),
    )
    for k in keys
        _push_row!(t, k, affinity === nothing ? nothing : affinity(k))
    end
    for v in values(t.pending)
        reverse!(v)
    end
    return t
end

# Appends a queued row; the caller owns the ordering of `pending` (see `add_tasks!`).
function _push_row!(t::TaskTable, key::DataKey, group)
    kstr = canonical(key)
    haskey(t.index, kstr) && return nothing
    push!(t.rows, TaskRow(key, kstr, group, :todo, nothing, nothing, 0, time(), nothing, 0))
    i = length(t.rows)
    t.index[kstr] = i
    push!(get!(Vector{Int}, t.pending, group), i)
    return i
end

"""
    add_tasks!(table, keys; affinity=nothing) -> Int

Queue `keys` behind what is already there. Keys already in the table are left alone. Returns how
many were added.
"""
function add_tasks!(t::TaskTable, keys::AbstractVector{DataKey}; affinity=nothing)
    return lock(t.lock) do
        n = 0
        for k in keys
            g = affinity === nothing ? nothing : affinity(k)
            i = _push_row!(t, k, g)
            i === nothing && continue
            # `_push_row!` appended, and the vector is popped from the end: move it to the front
            # so it comes out after everything queued earlier.
            v = t.pending[g]
            pop!(v)
            pushfirst!(v, i)
            n += 1
        end
        return n
    end
end

Base.length(t::TaskTable) = length(t.rows)

# Whether row `i` is still waiting in the queue. A row can leave the queue without being popped
# (cancelled, found done), so the queue holds indices and the row holds the truth.
_queued(t::TaskTable, i::Int) = t.rows[i].state === :todo

function _pop_live!(t::TaskTable, v::Vector{Int})
    while !isempty(v)
        i = pop!(v)
        _queued(t, i) && return i
    end
    return nothing
end

# The first queued row of `v` (from its end: the next to come out) that `accept`s, removed from
# `v`; rows that have left the queue are dropped on the way.
function _pop_accepted!(t::TaskTable, v::Vector{Int}, accept)
    j = length(v)
    while j >= 1
        i = v[j]
        if !_queued(t, i)
            deleteat!(v, j)
        elseif accept(t.rows[i])
            deleteat!(v, j)
            return i
        end
        j -= 1
    end
    return nothing
end

"""
    next_task!(table, worker; accept=nothing) -> Union{Int,Nothing}

The row index `worker` should take next, or `nothing` when the queue is empty. The row is NOT
marked running; call [`start_task!`](@ref) with the lock token.

Order: an urgent row first; then a row from a group this worker has already handled, most recent
group first; otherwise from the group with the most rows outstanding, which spreads workers over
groups. Without an affinity there is one group and this is the caller's order.

`accept` is `row -> Bool`: with it, the row returned is the first, in that order, that the worker
may take (a worker of one size passes over the keys it cannot hold); the rows passed over stay
queued, in place.
"""
function next_task!(t::TaskTable, worker::Int; accept=nothing)
    accept === nothing || return _next_accepted!(t, worker, accept)
    return lock(t.lock) do
        while !isempty(t.urgent)
            i = popfirst!(t.urgent)
            _queued(t, i) && return i
        end
        mine = get!(Vector{Any}, t.seen, worker)
        for (j, g) in enumerate(mine)
            v = get(t.pending, g, nothing)
            v === nothing && continue
            i = _pop_live!(t, v)
            if i !== nothing
                j == 1 || (deleteat!(mine, j); pushfirst!(mine, g))
                return i
            end
        end
        while true
            best, bestn = nothing, 0
            found = false
            for (g, v) in t.pending
                length(v) > bestn && ((best, bestn, found) = (g, length(v), true))
            end
            found || return nothing
            i = _pop_live!(t, t.pending[best])
            # The largest group held only rows that had already left the queue: it is empty now,
            # so look again.
            i === nothing && continue
            pushfirst!(mine, best)
            return i
        end
    end
end

function _next_accepted!(t::TaskTable, worker::Int, accept)
    return lock(t.lock) do
        for (j, i) in enumerate(t.urgent)
            _queued(t, i) && accept(t.rows[i]) || continue
            deleteat!(t.urgent, j)
            return i
        end
        mine = get!(Vector{Any}, t.seen, worker)
        for (j, g) in enumerate(mine)
            v = get(t.pending, g, nothing)
            v === nothing && continue
            i = _pop_accepted!(t, v, accept)
            i === nothing && continue
            j == 1 || (deleteat!(mine, j); pushfirst!(mine, g))
            return i
        end
        # Largest group first, as without `accept`.
        for g in sort!(collect(keys(t.pending)); by=g -> -length(t.pending[g]))
            i = _pop_accepted!(t, t.pending[g], accept)
            i === nothing && continue
            pushfirst!(mine, g)
            return i
        end
        return nothing
    end
end

"""
    start_task!(table, i, owner, worker)

Row `i` is handed to `worker`, which will take the lock as `owner`.
"""
function start_task!(t::TaskTable, i::Int, owner::AbstractString, worker::Int)
    lock(t.lock) do
        r = t.rows[i]
        r.state = :running
        r.owner = String(owner)
        r.worker = worker
        r.since = time()
        r.outcome = nothing
        return nothing
    end
    return nothing
end

# What a row can be settled with. A closed set: `run!` counts its result by these names, and one
# it does not know would be counted as "busy, retriable" without a word.
const OUTCOMES = (
    :ok,
    :already_done,
    :lock_busy,
    :deferred,
    :worker_lost,
    :error,
    :gave_up,
    :stop_flag,
    :stop_deadline,
    :stop_request,
    :stopped,
    :cancelled,
    :no_fit,
)

function _check_outcome(outcome::Symbol)
    outcome in OUTCOMES || throw(
        ArgumentError(
            "TaskTable: $(repr(outcome)) is not an outcome; one of $(join(repr.(OUTCOMES), ", "))",
        ),
    )
    return nothing
end

"""
    settle!(table, i, outcome)

This round is finished with row `i`. `outcome` is one of `SweepRunner.OUTCOMES`; anything else is
an `ArgumentError`.
"""
function settle!(t::TaskTable, i::Int, outcome::Symbol)
    _check_outcome(outcome)
    lock(t.lock) do
        r = t.rows[i]
        r.state = :settled
        r.outcome = outcome
        r.worker = 0
        r.since = time()
        outcome === :ok && (r.progress = nothing)
        return nothing
    end
    return nothing
end

"""
    hold!(table, i, owner)

Row `i` is locked by another master (`owner`, or `nothing` for an unstamped lock). It leaves the
queue and counts as `:lock_busy`.
"""
function hold!(t::TaskTable, i::Int, owner)
    lock(t.lock) do
        r = t.rows[i]
        r.state = :held
        r.outcome = :lock_busy
        r.owner = owner
        r.worker = 0
        r.since = time()
        return nothing
    end
    return nothing
end

"""
    requeue!(table, i; front=false)

Put row `i` back on the queue: behind the rest of its group, or ahead of everything with
`front=true`.
"""
function requeue!(t::TaskTable, i::Int; front::Bool=false)
    lock(t.lock) do
        r = t.rows[i]
        r.state = :todo
        r.outcome = nothing
        r.owner = nothing
        r.worker = 0
        r.since = time()
        if front
            push!(t.urgent, i)
        else
            pushfirst!(get!(Vector{Int}, t.pending, r.group), i)
        end
        return nothing
    end
    return nothing
end

"""
    settle_queued!(table, outcome) -> Int

Settle every row still queued with `outcome` (a stop: the keys it drops are attributed, not
silently absent). Returns how many.
"""
function settle_queued!(t::TaskTable, outcome::Symbol)
    _check_outcome(outcome)
    return lock(t.lock) do
        n = 0
        for r in t.rows
            r.state === :todo || continue
            r.state = :settled
            r.outcome = outcome
            r.since = time()
            n += 1
        end
        foreach(empty!, values(t.pending))
        empty!(t.urgent)
        return n
    end
end

"""
    task_counts(table) -> NamedTuple

`(; total, todo, running, held, done, failed, other)` at this instant. `done` is `:ok` plus
`:already_done`; `failed` is `:error` plus `:gave_up`.
"""
function task_counts(t::TaskTable)
    return lock(t.lock) do
        todo = running = held = done = failed = other = 0
        for r in t.rows
            if r.state === :todo
                todo += 1
            elseif r.state === :running
                running += 1
            elseif r.state === :held || r.outcome === :lock_busy
                # Held by a sibling, whether the scan saw the lock or a worker ran into it.
                held += 1
            elseif r.outcome === :ok || r.outcome === :already_done
                done += 1
            elseif r.outcome === :error || r.outcome === :gave_up
                failed += 1
            else
                other += 1
            end
        end
        return (; total=length(t.rows), todo, running, held, done, failed, other)
    end
end

# Exported: the names that say what they are. The rest of this file's API is documented and used
# qualified (`SweepRunner.next_task!`): a name that short or that common is not this package's to put in
# a caller's namespace.
export TaskTable
