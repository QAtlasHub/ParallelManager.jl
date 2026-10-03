# Checkpoint — what a key keeps when it stops, as a service of the per-key pipeline.
#
# What a key kept when it stopped was up to each application. Downstream a key was saved only at
# segment boundaries, so every wall clock, cancel, node loss and out-of-memory kill cost each
# running key up to one segment, on every job generation, on every worker: the largest single loss
# of compute in a chained campaign, and one each application had to solve again.
#
# Inside `work_fn`:
#
#     cp    = SweepRunner.checkpoint()
#     state = something(SweepRunner.load_checkpoint(cp), initial_state(key))
#     while !finished(state)
#         state = advance(state)
#         SweepRunner.checkpoint_due(cp) &&
#             SweepRunner.save_checkpoint!(cp, state; step = state.step, of = nsteps)
#         SweepRunner.stop_point()
#     end
#
# `checkpoint_due` is the one question the loop asks: it is true every `RunOpts.checkpoint_every`
# seconds, and at once when the unit has been told to stop or the job's deadline is close. The
# application does not implement the timing, the atomic write, the progress stamp or the clean-up.

using JLD2
using DataVault: Vault

"""
    Checkpoint

The handle [`checkpoint`](@ref) returns for the key a `work_fn` call is on. Outside a `run!` it is
inert: nothing to load, nothing saved, never due — so a `work_fn` written against it runs on its
own.
"""
struct Checkpoint
    ctx::Union{KeyContext,Nothing}
end

"""
    checkpoint() -> Checkpoint

The checkpoint handle of the key this `work_fn` call was handed. See
[`load_checkpoint`](@ref), [`save_checkpoint!`](@ref), [`checkpoint_due`](@ref).
"""
checkpoint() = Checkpoint(_KEY[])

"""
    checkpoint_dir(vault) -> String

`<state_root>/checkpoints`: one file per partly-done key, `<sha1 of the canonical key>.jld2`.
"""
checkpoint_dir(vault::Vault) = joinpath(state_root(vault), "checkpoints")

function _checkpoint_file(vault::Vault, kstr::AbstractString)
    return joinpath(checkpoint_dir(vault), _key_hash(kstr) * ".jld2")
end

"""
    load_checkpoint(cp) -> Union{Any,Nothing}

The state last saved for this key with [`save_checkpoint!`](@ref) — by an earlier attempt, on any
worker of any job — or `nothing` when there is none. A checkpoint that cannot be read (a
truncated file, a type that no longer loads after a code change) is `nothing` too: starting over
is always correct, resuming from a damaged file is not. It is not dropped quietly, though: the
file is moved aside (`….unreadable.<time>`) and a `checkpoint_unreadable` warning names the key,
the error and where the file went.
"""
function load_checkpoint(cp::Checkpoint)
    ctx = cp.ctx
    ctx === nothing && return nothing
    path = _checkpoint_file(ctx.vault, ctx.kstr)
    isfile(path) || return nothing
    try
        # `jldopen` rather than `load`: the latter prints its own error report before throwing.
        return JLD2.jldopen(f -> f["state"], path, "r")
    catch e
        e isa InterruptException && rethrow()
        # Starting over is correct; saying nothing and letting the next save overwrite a file
        # that might have been recoverable is not. It is kept aside and reported.
        aside = string(path, ".unreadable.", round(Int, time()))
        kept = try
            mv(path, aside; force=true)
            aside
        catch
            nothing
        end
        ctx.log === nothing || log_event(
            ctx.log,
            :checkpoint_unreadable;
            level=:warn,
            stage=ctx.stage,
            key=ctx.kstr,
            err=_short_err(e),
            kept=kept,
        )
        return nothing
    end
end

"""
    save_checkpoint!(cp, state; step=nothing, of=nothing, note="") -> Bool

Save `state` as this key's checkpoint, replacing the previous one. The write is atomic (a
temporary file, then a rename), so a kill in the middle leaves the previous checkpoint, never a
torn one.

With `step`, the progress stamp is written too ([`report_progress`](@ref)), which is what the
status, the lock listing and the account read: "alive and advancing", and how much of the key was
kept when it was cut. Without `step` the stamp counts the saves.

A unit that no longer holds its key (cut after a stop's grace, or reclaimed) does not save: the
call throws [`StopRequested`](@ref), so the unit leaves and the checkpoint of whoever holds the
key now is not replaced by older state.

Returns `false` and does nothing outside a `run!`. A save that fails throws: a `work_fn` that
believes it has a checkpoint it does not have would lose more than the one step.
"""
function save_checkpoint!(
    cp::Checkpoint, state; step::Union{Integer,Nothing}=nothing, of=nothing, note=""
)
    ctx = cp.ctx
    ctx === nothing && return false
    c = ctx.cp
    # The key is no longer this unit's: its state must not replace the new owner's checkpoint,
    # and it has nothing left to compute for. It leaves here, as at a stop.
    _still_owner(ctx) || throw(StopRequested())
    path = _checkpoint_file(ctx.vault, ctx.kstr)
    mkpath(dirname(path))
    # The extension stays `.jld2`: JLD2 picks its format from it.
    tmp = string(path[1:(end - 5)], ".tmp.", getpid(), ".", rand(UInt32), ".jld2")
    try
        JLD2.jldsave(tmp; state=state, saved_at=time())
        mv(tmp, path; force=true)
    finally
        rm(tmp; force=true)
    end
    c.saves += 1
    c.last = time()
    c.used = true
    c.near_saved = c.near
    report_progress(something(step, c.saves); of=of, note=note)
    c.mode === :cut && throw(CheckpointCut())
    return true
end

"""
    checkpoint_due(cp) -> Bool

Whether it is time to save: `RunOpts.checkpoint_every` seconds have passed since the last save (or
since the key started), or the unit has been told to stop ([`should_stop`](@ref): the job's flag,
its deadline, a `:stop` request), or the job's `deadline` is within one `checkpoint_every` (or a
minute, whichever is shorter) and nothing has been saved since it came that close.

Ask it once per step of the loop. It costs a clock read; the filesystem is consulted only as often
as `should_stop` does.
"""
function checkpoint_due(cp::Checkpoint)::Bool
    ctx = cp.ctx
    ctx === nothing && return false
    c = ctx.cp
    c.mode === :never && return false
    c.mode === :cut && return true
    now = time()
    every = ctx.opts.checkpoint_every
    (every > 0 && now - c.last >= every) && return true
    should_stop() && return true
    d = ctx.opts.deadline
    if d !== nothing && now >= d - min(every > 0 ? every : 60.0, 60.0)
        c.near = true
        c.near_saved || return true
    end
    return false
end

function _clear_checkpoint(vault::Vault, kstr::AbstractString)
    try
        rm(_checkpoint_file(vault, kstr); force=true)
    catch e
        e isa InterruptException && rethrow()
    end
    return nothing
end

# ── checking that a work function really resumes ────────────────────────────────────────────────

# Thrown by `save_checkpoint!` in the `:cut` mode of `check_checkpoints`: the key is cut right
# after every save.
struct CheckpointCut <: Exception end

function _call_with_checkpoints(work_fn, vault::Vault, key::DataKey, mode::Symbol)
    kstr = canonical(key)
    opts = RunOpts(; stop_flag=nothing, status_interval=0, control_interval=0)
    ctx = KeyContext(
        vault,
        key,
        kstr,
        "check",
        _read_progress_one(vault, kstr),
        opts,
        Ref(false),
        StopWatch(time(), "", ""),
        Dict{String,Any}(),
        CheckpointState(mode),
        nothing,
        Symbol(vault.run),
    )
    return with(() -> work_fn(key), _KEY => ctx)
end

"""
    check_checkpoints(work_fn, vault, key; max_restarts=10_000) -> NamedTuple

Run `work_fn(key)` twice and compare: once straight through with no checkpoint taken, and once
cut immediately after EVERY [`save_checkpoint!`](@ref) and restarted from that checkpoint until it
finishes. Returns `(; same, restarts, plain, resumed)`; `same` is `isequal(plain, resumed)`.

This is the test a checkpointing `work_fn` should pass: a result that depends on where the key
was interrupted is a wrong result that only shows up on a cluster. `vault` is scratch (nothing is
committed to it; the checkpoint and progress files written under it are removed).

```julia
r = SweepRunner.check_checkpoints(work_fn, scratch_vault, key)
@test r.same
@test r.restarts > 0        # it did checkpoint
```
"""
function check_checkpoints(
    work_fn, vault::Vault, key::DataKey; max_restarts::Integer=10_000
)
    kstr = canonical(key)
    clean() = (_clear_checkpoint(vault, kstr); _clear_progress(vault, kstr))
    clean()
    plain = try
        _call_with_checkpoints(work_fn, vault, key, :never)
    finally
        clean()
    end
    restarts = Ref(0)
    resumed = try
        _run_cut(work_fn, vault, key, restarts, max_restarts)
    finally
        clean()
    end
    return (;
        same=isequal(plain, resumed), restarts=restarts[], plain=plain, resumed=resumed
    )
end

# Run the key, cut after every save, until one run gets to the end.
function _run_cut(work_fn, vault::Vault, key::DataKey, restarts, max_restarts)
    while true
        try
            return _call_with_checkpoints(work_fn, vault, key, :cut)
        catch e
            e isa CheckpointCut || rethrow()
            restarts[] += 1
            restarts[] > max_restarts && error(
                "check_checkpoints: $max_restarts restarts without finishing; the work " *
                "function does not advance from its checkpoint",
            )
        end
    end
end

# `checkpoint()` and `Checkpoint` are used qualified (`SweepRunner.checkpoint()`).
export load_checkpoint, save_checkpoint!, checkpoint_due, check_checkpoints
