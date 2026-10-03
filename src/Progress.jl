# Progress — how far a unit got, recorded where the next attempt is handed it.
#
# A unit that is made of steps (time segments, sweeps, checkpoints) used to find out where to
# resume by probing its own outputs, one step at a time, on whichever worker picked it up. The
# step count is recorded here instead, in one small file per partly-done unit, in ONE directory:
# the master reads that directory once and hands each worker "resume from step k" with the key.
#
# The same stamp is what tells "alive" from "advancing": a lock's heartbeat is written by a child
# process and keeps beating through a computation that is stuck.

using Base.ScopedValues: ScopedValue, with
using SHA: sha1
using JSON3
using DataVault: Vault
using ParamIO: DataKey, canonical

"""
    KeyContext

What a worker was handed with a key. Present (through a scoped value, so tasks `work_fn` spawns
see it too) only while `work_fn(key)` runs under [`run!`](@ref).
"""
struct KeyContext
    vault::Vault
    key::DataKey
    kstr::String
    owner::String
    resume::Union{Progress,Nothing}
    opts::RunOpts
    # Whether a progress stamp exists for this key (handed in, or written during this call).
    reported::Base.RefValue{Bool}
    # What `should_stop` needs to recognise a stop request that covers this unit.
    watch::StopWatch
    # What the application adds to this key's `key_done` record (`note_key!`).
    notes::Dict{String,Any}
    # The key's checkpoint bookkeeping for this call (`checkpoint_due`, `save_checkpoint!`).
    cp::CheckpointState
    # Where this call's warnings go (`nothing` outside the per-key pipeline).
    log::Union{EventLog,Nothing}
    stage::Symbol
end

const _KEY = ScopedValue{Union{KeyContext,Nothing}}(nothing)

"""
    progress_dir(vault) -> String

The one directory holding this `(project, run)`'s progress stamps,
`<state_root>/progress/<sha1 of the canonical key>.json`.
"""
progress_dir(vault::Vault) = joinpath(state_root(vault), "progress")

_key_hash(kstr::AbstractString) = bytes2hex(sha1(String(kstr)))

function _progress_file(vault::Vault, kstr::AbstractString)
    return joinpath(progress_dir(vault), _key_hash(kstr) * ".json")
end

"""
    report_progress(step; of=nothing, note="") -> Bool

Record, from inside `work_fn`, that this unit has finished `step` (out of `of`, when known). Call
it AFTER whatever makes the step resumable is on disk: the next attempt at this key, on any worker
of any job, gets the value back from [`resume_point`](@ref).

Returns `false` and does nothing outside a `run!`, so a `work_fn` that calls it stays runnable on
its own. It also returns `false`, and writes nothing, when the unit no longer holds its key (it
was cut after a stop's grace, or its lock was reclaimed): the stamp belongs to whoever holds the
key now. A write that fails is not an error either: progress is advice to the next attempt, and
losing it costs a re-probe, not a result.
"""
function report_progress(step::Integer; of::Union{Integer,Nothing}=nothing, note="")
    ctx = _KEY[]
    ctx === nothing && return false
    # A unit that no longer holds its key (it was cut, or reclaimed) must not write over the
    # stamp of whoever holds it now.
    _still_owner(ctx) || return false
    try
        _write_progress(ctx.vault, ctx.kstr, Progress(step, of, time(), String(note)))
        ctx.reported[] = true
        return true
    catch e
        e isa InterruptException && rethrow()
        return false
    end
end

"""
    resume_point() -> Union{Progress,Nothing}

The last [`report_progress`](@ref) recorded for the key this `work_fn` call was handed, or
`nothing` when the unit has not reported any (or outside a `run!`). The master read it when it
built its task table, so this is a field lookup, not a filesystem probe.

```julia
function work_fn(key)
    p = SweepRunner.resume_point()
    first = p === nothing ? 1 : p.step + 1
    for seg in first:nseg
        run_segment!(key, seg)            # writes its own checkpoint
        SweepRunner.report_progress(seg; of=nseg)
    end
    return collect_result(key)
end
```
"""
function resume_point()
    ctx = _KEY[]
    return ctx === nothing ? nothing : ctx.resume
end

# Does this `work_fn` call still hold its key's lock? Outside the per-key pipeline
# (`check_checkpoints`) there is no lock to hold.
function _still_owner(ctx::KeyContext)::Bool
    ctx.cp.mode === :normal || return true
    return DataVault.running_owner(ctx.vault, ctx.key) == ctx.owner
end

function _write_progress(vault::Vault, kstr::AbstractString, p::Progress)
    atomic_write(_progress_file(vault, kstr)) do io
        return JSON3.write(io, (; key=kstr, step=p.step, of=p.of, at=p.at, note=p.note))
    end
    return nothing
end

function _clear_progress(vault::Vault, kstr::AbstractString)
    try
        rm(_progress_file(vault, kstr); force=true)
    catch e
        e isa InterruptException && rethrow()
    end
    return nothing
end

function _parse_progress(text::AbstractString)
    j = JSON3.read(text)
    of = get(j, :of, nothing)
    p = Progress(
        Int(j.step),
        of === nothing ? nothing : Int(of),
        Float64(j.at),
        String(get(j, :note, "")),
    )
    return String(j.key), p
end

# One key's stamp, or `nothing`: absent, unreadable and unparsable are the same answer.
function _read_progress_one(vault::Vault, kstr::AbstractString)
    try
        f = _progress_file(vault, kstr)
        isfile(f) || return nothing
        return last(_parse_progress(read(f, String)))
    catch e
        e isa InterruptException && rethrow()
        return nothing
    end
end

"""
    read_progress(vault) -> Dict{String,Progress}

Every recorded [`Progress`](@ref) of this `(project, run)`, by canonical key. One directory
listing plus one read per partly-done unit; a file that cannot be parsed is skipped and counted
in `unreadable`.
"""
function read_progress(vault::Vault; unreadable::Base.RefValue{Int}=Ref(0))
    out = Dict{String,Progress}()
    dir = progress_dir(vault)
    isdir(dir) || return out
    for f in readdir(dir)
        endswith(f, ".json") || continue
        try
            k, p = _parse_progress(read(joinpath(dir, f), String))
            out[k] = p
        catch e
            e isa InterruptException && rethrow()
            # Counted, so the caller can say that some keys will start without their resume point.
            unreadable[] += 1
        end
    end
    return out
end

export report_progress, resume_point, read_progress
