# Manifest — rollup index of completed keys per stage.
#
# A Manifest turns the "is this 3600-key run already done?" question from an
# O(N) filesystem scan into an O(1) JLD2 read. FiniteTemperature.jl used to
# stat 3600 `.done` files on every job startup (~10 minutes wall clock); a
# Manifest closes that loop in under 10 ms.

using JLD2
using ParamIO: DataKey, canonical

"""
    Manifest(stage, root, complete)
    Manifest(stage, root)

Rollup index for a single stage under a vault root. `complete` holds
[`ParamIO.canonical`](@extref) strings of every key known to be finished.

# Fields

- `stage::Symbol` — stage label, e.g. `:phase1`. Used to pick the
  sub-directory inside `root`.
- `root::String`  — the vault root (typically `vault.outdir`) combined
  with the project name. See [`manifest_root`](@ref) for the convenience
  overload.
- `complete::Set{String}` — in-memory set of canonical key strings,
  rehydrated from the JLD2 file on load.

# On-disk format

    <root>/<stage>/manifest.jld2

JLD2 layout: one field `complete::Vector{String}`. A `Vector` is used (not
a `Set`) so older JLD2 versions deserialize cleanly; `load_manifest`
rehydrates to a `Set{String}` for O(1) lookup.

# Invariants

- **Monotonic.** Once a key is added via [`add_complete!`](@ref) it is
  never removed by this package. Re-computing the same root with different
  work is considered a contract violation — branch into a new `outdir`.
- **Atomic updates.** [`save_manifest`](@ref) uses a tmp + rename dance so
  a crash mid-write cannot leave a half-written manifest on disk.
- **Corrupt file = empty.** If a manifest file exists but cannot be parsed
  by JLD2, [`load_manifest`](@ref) returns an empty `Manifest`. The worst
  outcome is re-running already-complete keys, which is idempotent thanks
  to the per-key `.running` lock and the `is_done` re-check in [`run!`](@ref).

# Typical use from inside `run!`

    m = load_manifest(vault)
    todo = todo_keys(m, collect(keys))
    isempty(todo) && return :skip
    for key in todo
        # ... do work ...
        add_complete!(m, key)
    end
    save_manifest(m)

# See also

[`load_manifest`](@ref), [`save_manifest`](@ref), [`add_complete!`](@ref),
[`is_complete`](@ref), [`todo_keys`](@ref), [`manifest_path`](@ref),
[`manifest_root`](@ref).
"""
mutable struct Manifest
    stage::Symbol
    root::String
    complete::Set{String}
end

Manifest(stage::Symbol, root::AbstractString) = Manifest(stage, String(root), Set{String}())

"""
    manifest_path(root, stage) -> String

Return the conventional on-disk location `"\$root/\$stage/manifest.jld2"`.
Pure function; does not touch the filesystem.
"""
function manifest_path(root::AbstractString, stage::Symbol)
    return joinpath(String(root), String(stage), "manifest.jld2")
end

"""
    load_manifest(root, stage) -> Manifest
    load_manifest(vault::DataVault.Vault) -> Manifest

Read `manifest.jld2` if present; otherwise return an empty [`Manifest`](@ref).

A corrupted manifest (any `JLD2.load` failure) is treated as empty. This
prefers making progress over refusing to run — the runtime's `is_done`
re-check after lock-acquire makes accidental re-processing harmless.

The second form is a convenience defined in `Run.jl` that derives
`(root, stage)` from a `DataVault.Vault` (see [`manifest_root`](@ref)).
"""
function load_manifest(root::AbstractString, stage::Symbol)::Manifest
    p = manifest_path(root, stage)
    if !isfile(p)
        return Manifest(stage, root)
    end
    try
        data = JLD2.load(p)
        items = get(data, "complete", String[])
        return Manifest(stage, String(root), Set{String}(items))
    catch
        # Treat corrupted manifest as empty — worst case we re-run some keys,
        # which the lock-acquire + is_done re-check makes safe.
        return Manifest(stage, String(root))
    end
end

"""
    save_manifest(m) -> String

Atomically write `m.complete` to `manifest_path(m.root, m.stage)` and
return that path.

Internally uses a `JLD2.jldsave` to a sibling tmp path followed by
`mv(..., force=true)`, so a crash in the middle leaves either the old
manifest (unchanged) or the new one — never a truncated JLD2.

`m.complete` is sorted on save so the on-disk Vector has a deterministic
order, making `jld2` files easier to diff for debugging.
"""
function save_manifest(m::Manifest)
    p = manifest_path(m.root, m.stage)
    items = sort!(collect(m.complete))
    mkpath(dirname(p))
    # JLD2 writes to a path, so we do the tmp + rename dance directly.
    tmp = string(p, ".tmp.", getpid(), ".", rand(UInt32))
    try
        JLD2.jldsave(tmp; complete=items)
        mv(tmp, p; force=true)
    catch
        isfile(tmp) && try
            rm(tmp; force=true)
        catch
        end
        rethrow()
    end
    return p
end

"""
    add_complete!(m, key) -> Set{String}

Add `ParamIO.canonical(key)` to the in-memory `m.complete` set. Not
persisted until [`save_manifest`](@ref) is called.

This is a one-way operation: the `Manifest` API does not expose a
`remove_complete!` (see the monotonic invariant in the [`Manifest`](@ref)
docstring).
"""
add_complete!(m::Manifest, key::DataKey) = push!(m.complete, canonical(key))

"""
    is_complete(m, key) -> Bool

`true` if `ParamIO.canonical(key)` is in `m.complete`. This is the
per-key check used by [`todo_keys`](@ref); it never touches the filesystem.
"""
is_complete(m::Manifest, key::DataKey) = canonical(key) in m.complete

"""
    todo_keys(m, keys) -> Vector{DataKey}

Return the subset of `keys` that are **not** yet in `m.complete`. This is
the work list for the next [`run!`](@ref); a fully-done run returns an
empty vector, which triggers the early-skip path.

Cost is O(length(keys)) hash lookups — independent of filesystem state.
On 3600 keys this takes ~6–12 ms in the benchmark.
"""
function todo_keys(m::Manifest, keys::AbstractVector{DataKey})
    return [k for k in keys if !is_complete(m, k)]
end

"""
    merge_and_save_manifest!(m) -> String

Reload the on-disk manifest, merge its completed keys into `m`, then
atomically persist. This prevents the "last writer wins" problem when
multiple masters call `save_manifest` concurrently — without the merge,
each master would overwrite the others' newly completed keys.

There is a small TOCTOU window between the `load_manifest` and the
`save_manifest`; in the worst case a concurrent saver's keys are not
included in this write, but they will be re-processed on the next
invocation (safe due to the `is_done` re-check after lock-acquire).
"""
function merge_and_save_manifest!(m::Manifest)
    on_disk = load_manifest(m.root, m.stage)
    union!(m.complete, on_disk.complete)
    return save_manifest(m)
end

export Manifest, manifest_path, load_manifest, save_manifest, merge_and_save_manifest!
export add_complete!, is_complete, todo_keys
