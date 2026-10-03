# Observe.jl — one source observation per process per run!, handed to every `.done` it writes.
#
# `DataVault.observe_sources` records what the source looked like and how far THIS process's loaded
# code was checked against it. A point's marker must carry the token of the process that computed
# it: with worker processes that is the worker, whose loaded code need not be the master's. So each process
# observes for itself at `run!` start, keeps the token here, and `_run_one_with_retry!` — which runs
# on that same process — reads it back.
#
# Keyed by vault identity, not held in one slot, so two `run!`s sharing a process never hand each
# other's token to a marker.

const _OBSERVATIONS = Dict{Tuple{String,String,String},String}()
const _OBSERVATIONS_LOCK = ReentrantLock()

_observation_key(vault::Vault) = (vault.outdir, vault.spec.study.project_name, vault.run)

"""
    _observe_here!(vault, role) -> (token, err)

Observe the sources from this process and remember the token for `vault`. On failure the token is
`nothing` and any earlier token for `vault` is forgotten, so a marker written afterwards says
`observation=unknown` rather than naming an observation of some earlier state.
"""
function _observe_here!(vault::Vault, role::AbstractString)
    token, err = try
        DataVault.observe_sources(
            vault;
            phase="run-start",
            process=Dict("role" => String(role), "myid" => myid()),
        ),
        nothing
    catch e
        nothing, sprint(showerror, e)
    end
    lock(_OBSERVATIONS_LOCK) do
        if token === nothing
            delete!(_OBSERVATIONS, _observation_key(vault))
        else
            _OBSERVATIONS[_observation_key(vault)] = token
        end
    end
    return token, err
end

"Forget this process's token for `vault` (a `run!` with `observe=false`)."
function _forget_observation!(vault::Vault)
    lock(() -> delete!(_OBSERVATIONS, _observation_key(vault)), _OBSERVATIONS_LOCK)
    return nothing
end

"This process's token for `vault`, or `nothing`."
function _observation_token(vault::Vault)
    return lock(
        () -> get(_OBSERVATIONS, _observation_key(vault), nothing), _OBSERVATIONS_LOCK
    )
end

# Observe on the master, and on every worker when the run fans out. Each outcome is an event: an
# observation that failed leaves its process's markers at `observation=unknown`, and says why.
function _observe_processes!(vault::Vault, multi::Bool, observe::Bool, log, stage)
    targets = if multi
        vcat([(myid(), "master")], [(w, "worker") for w in workers()])
    else
        [(myid(), "master")]
    end
    if !observe
        for (pid, _) in targets
            if pid == myid()
                _forget_observation!(vault)
            else
                remotecall_fetch(SweepRunner._forget_observation!, pid, vault)
            end
        end
        return nothing
    end
    outcomes = asyncmap(targets) do (pid, role)
        return if pid == myid()
            _observe_here!(vault, role)
        else
            remotecall_fetch(SweepRunner._observe_here!, pid, vault, role)
        end
    end
    for ((pid, role), (token, err)) in zip(targets, outcomes)
        if token === nothing
            log_event(
                log, :observe_failed; level=:warn, stage=stage, pid=pid, role=role, err=err
            )
        else
            log_event(log, :observed; stage=stage, pid=pid, role=role, token=token)
        end
    end
    return nothing
end

# The same, for workers that joined after the round began (see `_drive_workers!`).
function _observe_late!(vault::Vault, pids, observe::Bool, log, stage)
    for pid in pids
        if !observe
            remotecall_fetch(SweepRunner._forget_observation!, pid, vault)
            continue
        end
        token, err = remotecall_fetch(SweepRunner._observe_here!, pid, vault, "worker")
        if token === nothing
            log_event(
                log,
                :observe_failed;
                level=:warn,
                stage=stage,
                pid=pid,
                role="worker",
                err=err,
            )
        else
            log_event(log, :observed; stage=stage, pid=pid, role="worker", token=token)
        end
    end
    return nothing
end
