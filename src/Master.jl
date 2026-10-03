# Master — what one `run!` / `run_loop!` invocation knows about itself and its workers.
#
# A master is the process that called `run!`. Several of them can share a vault (one per batch
# job), so everything a master publishes lives under its own directory and nothing here is a
# singleton.

using Distributed
using DataVault: Vault

"""
    state_root(vault) -> String

Where SweepRunner keeps its own state for one `(project, run)`:

    <vault.outdir>/sweeprunner/<project_name>/<vault.run>/

Pure function; does not touch the filesystem.
"""
function state_root(vault::Vault)
    return joinpath(vault.outdir, "sweeprunner", vault.spec.study.project_name, vault.run)
end

"""
    Master()

One master's identity and the worker identities it has collected. [`run_loop!`](@ref) builds one
and keeps it across its rounds; a bare [`run!`](@ref) builds its own.

- `id` — `<hostname>_<pid>`, the same pair that names the master's event log.
- `started` — `time()` at construction.
- `who` — Distributed id => `(hostname, os pid)` of each worker asked so far.
"""
mutable struct Master
    const id::String
    const started::Float64
    const who::Dict{Int,Tuple{String,Int}}
    const lock::ReentrantLock
end

function Master()
    return Master(
        string(gethostname(), "_", getpid()),
        time(),
        Dict{Int,Tuple{String,Int}}(),
        ReentrantLock(),
    )
end

_whoami() = (gethostname(), getpid())

# Ask the workers not asked yet who they are, all at once. A worker that cannot answer is left out,
# and the dispatcher does not hand it work: it could not be named in a lock.
function _identify_workers!(m::Master, pids)
    unknown = lock(() -> [p for p in pids if !haskey(m.who, p)], m.lock)
    isempty(unknown) && return nothing
    answers = asyncmap(unknown) do p
        try
            p == myid() ? _whoami() : remotecall_fetch(_whoami, p)
        catch e
            e isa InterruptException && rethrow()
            nothing
        end
    end
    lock(m.lock) do
        for (p, a) in zip(unknown, answers)
            a === nothing || (m.who[p] = a)
        end
    end
    return nothing
end

export state_root, Master
