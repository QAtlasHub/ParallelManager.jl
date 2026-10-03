# Account — where a job's core-hours went.
#
# A job is charged nodes × elapsed time. How much of that was keys advancing was not known for any
# job: the start-up before it was at full size, the workers that never started, the tail, the
# work since the last checkpoint thrown away at every job end, and the time spent waiting on
# other jobs' locks were each found by reading logs by hand, by accident.
#
# The master keeps the account as it dispatches, in core-seconds:
#
#   computing      a worker had a key
#     kept           ... and the key finished, or had reported progress up to that point
#     lost           ... and the key was cut, stopped, failed or its worker died, after its last
#                    reported progress
#   start-up       a worker's cores before its first key
#   never started  cores of the workers that were planned and did not join
#   idle           a worker that had joined had no key, by reason
#
# It goes to the event log when the master ends (`job_account`), to the status while it runs, and
# `print_account` / `sweeprunner account` print it.

"""
    Account

One master's running account, in core-seconds: `busy` (a worker had a key), split into `kept` and
`lost`; `idle` by reason (`:queue_empty`, `:lock_busy`, `:paused`, `:stopping`); per worker when
it `joined` and when it got its `first_key`; and `keys_cut`, how many keys ended without
finishing.
"""
mutable struct Account
    busy::Float64
    kept::Float64
    lost::Float64
    keys_cut::Int
    const idle::Dict{Symbol,Float64}
    const joined::Dict{Int,Float64}
    const first_key::Dict{Int,Float64}
    const cores::Dict{Int,Int}
end

function Account()
    return Account(
        0.0,
        0.0,
        0.0,
        0,
        Dict{Symbol,Float64}(),
        Dict{Int,Float64}(),
        Dict{Int,Float64}(),
        Dict{Int,Int}(),
    )
end

# A worker is known to the master from here on.
function _acct_join!(a::Account, worker::Int, cores::Int; at::Float64=time())
    haskey(a.joined, worker) && return nothing
    a.joined[worker] = at
    a.cores[worker] = max(cores, 1)
    return nothing
end

# A key ran on `worker` from `t0` to `t1`. `finished` says it completed; otherwise `progress_at`
# is when it last reported progress during this attempt (`nothing`: it never did), and everything
# after that is lost. `cut` says whether the key had started work that was thrown away (as
# opposed to coming straight back because someone else held it).
function _acct_key!(
    a::Account,
    worker::Int,
    t0::Float64,
    t1::Float64,
    finished::Bool,
    progress_at::Union{Float64,Nothing};
    cut::Bool=true,
)
    c = get(a.cores, worker, 1)
    get!(a.first_key, worker, t0)
    span = max(t1 - t0, 0.0) * c
    a.busy += span
    if finished
        a.kept += span
    else
        saved = progress_at === nothing ? t0 : clamp(progress_at, t0, t1)
        a.kept += (saved - t0) * c
        a.lost += (t1 - saved) * c
        cut && (a.keys_cut += 1)
    end
    return nothing
end

# `worker` had no key from `t0` to `t1`, for `reason`.
function _acct_idle!(a::Account, worker::Int, reason::Symbol, t0::Float64, t1::Float64)
    c = get(a.cores, worker, 1)
    a.idle[reason] = get(a.idle, reason, 0.0) + max(t1 - t0, 0.0) * c
    return nothing
end

"""
    account_snapshot(master; now=time()) -> Dict{String,Any}

The account so far, in core-seconds (`elapsed` in seconds):

- `allocated` — the allocation's cores (`SLURM_JOB_CPUS_PER_NODE`, else the cores of the workers
  that joined) × `elapsed`;
- `computing`, `kept`, `lost`, `keys_cut`;
- `startup` — each worker's cores from the master's start to that worker's first key (to `now`
  for a worker that has had none);
- `never_started` — (workers planned − workers joined) × the mean cores of a worker × `elapsed`;
- `idle` — by reason, the time workers that had already had a key spent without one;
- `other` — what is left of `allocated`: the master itself, cores no worker was given.

`never_started` and `other` are estimates (the first assumes the missing workers were missing all
along); the rest is measured at each dispatch.
"""
function account_snapshot(m; now::Float64=time())
    a = m.acct
    elapsed = max(now - m.started, 0.0)
    startup = 0.0
    for (w, c) in a.cores
        startup += c * (get(a.first_key, w, now) - m.started)
    end
    joined_cores = sum(values(a.cores); init=0)
    planned, _, _ = lock(() -> _SPAWN[], _SPAWN_LOCK)
    njoined = length(a.cores)
    mean_cores = njoined == 0 ? 1.0 : joined_cores / njoined
    never = max(planned - njoined, 0) * mean_cores * elapsed
    alloc_cores = _slurm_alloc_cores()
    alloc_cores == 0 && (alloc_cores = joined_cores)
    allocated = alloc_cores * elapsed
    idle = sum(values(a.idle); init=0.0)
    return Dict{String,Any}(
        "elapsed" => elapsed,
        "allocated" => allocated,
        "computing" => a.busy,
        "kept" => a.kept,
        "lost" => a.lost,
        "keys_cut" => a.keys_cut,
        "startup" => startup,
        "never_started" => never,
        "idle" => Dict{String,Any}(String(k) => v for (k, v) in a.idle),
        "other" => max(allocated - a.busy - startup - never - idle, 0.0),
    )
end

_core_hours(x) = string(round(Float64(x) / 3600; digits=1))

function _account_line(io::IO, label, value, total; indent="")
    pct = total > 0 ? string(" (", round(Int, 100 * value / total), "%)") : ""
    return println(
        io,
        indent,
        rpad(label, 16 - length(indent)),
        lpad(_core_hours(value), 10),
        " core-h",
        pct,
    )
end

"""
    print_account([io], account::AbstractDict)
    print_account([io], vault_or_outdir)

Print an account ([`account_snapshot`](@ref)), or every master's last one under a vault or an
outdir (from their status files):

```
allocated         7632.0 core-h
computing         2890.0 core-h (38%)
  kept            2410.0 core-h (32%)
  lost             480.0 core-h (6%)   17 key(s) cut
start-up           410.0 core-h (5%)
never started     3960.0 core-h (52%)
idle               350.0 core-h (5%)
  queue_empty      290.0 core-h (4%)
  lock_busy         60.0 core-h (1%)
```
"""
print_account(x) = print_account(stdout, x)

function print_account(io::IO, a::AbstractDict)
    total = Float64(a["allocated"])
    _account_line(io, "allocated", total, 0.0)
    _account_line(io, "computing", a["computing"], total)
    _account_line(io, "kept", a["kept"], total; indent="  ")
    pct = total > 0 ? string(" (", round(Int, 100 * a["lost"] / total), "%)") : ""
    println(
        io,
        "  ",
        rpad("lost", 14),
        lpad(_core_hours(a["lost"]), 10),
        " core-h",
        pct,
        "   ",
        a["keys_cut"],
        " key(s) cut",
    )
    _account_line(io, "start-up", a["startup"], total)
    _account_line(io, "never started", a["never_started"], total)
    idle = a["idle"]
    _account_line(io, "idle", sum(values(idle); init=0.0), total)
    for k in sort!(collect(keys(idle)))
        _account_line(io, k, idle[k], total; indent="  ")
    end
    _account_line(io, "other", a["other"], total)
    return nothing
end

function print_account(io::IO, x)
    all = [d for d in read_status(x) if get(d, "account", nothing) !== nothing]
    if isempty(all)
        println(io, "no account found")
        return nothing
    end
    for d in all
        job = isempty(d["job"]) ? "" : " job $(d["job"])"
        hours = round(d["account"]["elapsed"] / 3600; digits=2)
        println(io, d["stage"], "  ", d["master"], job, "  ", d["state"], "  $hours h")
        print_account(io, d["account"])
    end
    return nothing
end

export account_snapshot, print_account
