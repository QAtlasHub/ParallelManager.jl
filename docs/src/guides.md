# Guides

Recipes for common situations.

## 1. Analyzing the event log

Each master writes its own log, `events_<host>_<pid>.jsonl` in the vault's
`outdir`, one JSON object per line. Read them together with a glob, or merge
them first with [`merge_event_logs`](@ref SweepRunner.merge_event_logs), which
writes `events_merged.jsonl` in time order. Any JSONL-aware tool works.

With `jq`:

```bash
# How many keys completed this session?
jq -c 'select(.kind == "key_done")' out/events_*.jsonl | wc -l

# Slowest 10 keys by wall-clock:
jq -c 'select(.kind == "key_done") | {key, secs}' out/events_*.jsonl \
  | jq -s 'sort_by(-.secs) | .[:10]'

# Lock contention across all masters:
jq -c 'select(.kind == "lock_busy") | .key' out/events_*.jsonl | sort | uniq -c | sort -nr
```

With Julia + `DataFrames`:

```julia
using JSON3, DataFrames

logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir("out"))
rows = [JSON3.read(l) for f in logs for l in readlines(joinpath("out", f))]
done = [(key=r.key, secs=r.secs) for r in rows if r.kind == "key_done"]
df = DataFrame(done)
sort!(df, :secs, rev=true)
```

## 2. Running multiple masters

Same vault, multiple `julia` processes:

```bash
for i in 1 2 3 4; do
    julia --project run.jl &
done
wait
```

The per-key `.running` lock arbitrates. Expect `:lock_busy`
events in proportion to contention. Eventually every key is done,
regardless of which master happened to win each race.

## 3. Recovering from a crashed master

If a master is `kill -9`'d (or its node reboots) mid-stage:

1. Its `.running` markers remain on disk, and their heartbeat stops.
2. Half-written payload files do not exist — [`atomic_write`](@ref SweepRunner.atomic_write)
   renames only after `fsync`, so readers see either the previous version
   or the new one.
3. Start a new master with the same `run.jl`. Before it builds its queue it
   asks who holds each lock (guide 10). A lock whose holder can be shown to be
   gone — its master's status no longer lists it, its Slurm job has ended, its
   pid is not there — is removed at once and the key re-runs. Where nobody can
   be asked, the heartbeat's age decides: after `opts.stale_after` seconds
   (default 600) the lock is reclaimed.

`stale_after` is that last resort, not the usual wait. For tests, tighten it:

```julia
SweepRunner.run!(work_fn, vault, keys;
                     opts=RunOpts(stale_after=1.0, heartbeat_interval=0.2))
```

## 4. Incremental re-runs

Adding a new parameter point to `config.toml` and re-running:

```diff
 [[paramsets]]
 N = [4, 8, 16]
-J = [0.5, 1.0]
+J = [0.5, 1.0, 2.0]
```

The existing `(N, J=0.5)` and `(N, J=1.0)` keys are already in the
manifest; only the new `(N, J=2.0)` keys run. Check the resulting
event log — you should see `:key_done` only for the new keys.

## 5. Debugging a single key

Bypass the whole runtime and call `work_fn` directly:

```julia
julia> using ParamIO, DataVault
julia> spec  = ParamIO.load("config.toml")
julia> keys  = ParamIO.expand(spec)
julia> vault = DataVault.Vault("config.toml"; run="phase1")
julia> work_fn(keys[1])
Dict{String, Any} with 3 entries:
  "N"      => 4
  "J"      => 0.5
  "energy" => 2.0
```

Because `work_fn` is pure, you can `@enter work_fn(keys[1])` or
`@infiltrate` inside it with no interference from the runtime.

## 6. Custom retry policy

```julia
SweepRunner.run!(work_fn, vault, keys;
                     opts=RunOpts(
                         max_attempts=5,
                         stale_after=1800.0,          # 30 min; only for locks nobody answers for
                         heartbeat_interval=120.0,     # 2 min
                     ))
```

`max_attempts=1` disables retry: a failure is logged as `:error` (not
`:gave_up`) and the key remains outside the manifest so the next `run!`
picks it up.

## 7. Running without SLURM

On a workstation or laptop:

```julia
SweepRunner.init_workers!(mode=:sequential, verbose=false)
SweepRunner.run!(work_fn, vault, keys)
```

or with multi-threading:

```bash
julia --project --threads=8 run.jl
```

`init_workers!(mode=:auto)` will pick `:threads` based on
`Threads.nthreads() > 1`.

## 8. Cleaning a corrupted manifest

A corrupted `manifest.jld2` is treated as empty by
[`load_manifest`](@ref SweepRunner.load_manifest), so the worst case
is a full re-run (made safe by per-key locks + `is_done` re-check). If
you want to force that:

```bash
rm out/manifest/<project>/<run>/manifest.jld2
```

Per-key `.done` files written by `DataVault.mark_done!` are the
authoritative source of truth — the manifest is just a cache.

## 9. Asking a running sweep what it is doing

Every master rewrites one file, atomically, every `RunOpts.status_interval`
seconds (60 by default):

    <outdir>/sweeprunner/<project>/<run>/masters/<host>_<pid>/status.json

Read it from a login node while the job runs, or after it has ended:

```sh
bin/sweeprunner status out/campaign            # every master under the outdir
bin/sweeprunner status out/campaign --workers  # plus one line per worker
bin/sweeprunner status out/campaign --json     # the raw records
```

```
phase1  c001_41233 job 3087883  running  updated 12 s ago
  tasks    total 9000  done 2400  running 1743  todo 4857  held 0  failed 0
  workers  planned 3735  launched 1782  joined 1743  busy 1743  idle 0
  cores    busy 3486 of 9216 allocated (38%)
  nodes    8 allocated with no worker: c065 c066 c067 c068 c069 c070 c071 c072
  ! workers_short: planned 3735, launched 1782, joined 1743 for 1260 s
  node                 workers  busy  cores   cpu
  c001                      28    28     56  0.97
  ...
```

or from Julia: `SweepRunner.read_status(vault)` returns the same records as
`Dict`s, `print_status(vault)` prints them.

Per worker the record holds the key it is on, since when, the lock token it
holds, the progress last reported for that key
([`report_progress`](@ref SweepRunner.report_progress)), CPU utilisation (CPU
time between two readings, over wall time, over the worker's cores) and RSS.
A worker inside a `work_fn` that never yields answers the reading when it next
does; `sampled` says when that was.

**Planned against joined.** The master knows how many workers joined. It does
not know how many were meant to, unless whatever starts them says so:

```julia
SweepRunner.note_workers!(planned = 3735)      # when the pool is sized
SweepRunner.note_workers!(launched = n_steps)  # as job steps come up
```

[`init_workers!`](@ref SweepRunner.init_workers!) does this for the pools it
starts. When fewer have joined than were planned for longer than the worker
timeout (`JULIA_WORKER_TIMEOUT`, else 60 s), the status carries a
`workers_short` line and the event log one `workers_short` event at `:warn` —
the ramp-up that stops at half its workers no longer does so silently.

A master whose file has not been rewritten for three intervals, and which did
not write `ended`, is shown as `GONE`: killed at the wall clock, or its node
was lost.

## 10. Whose locks are these?

A `.running` file says a key is being computed. Whether that is still true
used to be guessed from the heartbeat's age and from `squeue`. It is now asked
of the holder's master: a master names every lock before its worker takes it
and lists the ones it has out in its status file, so

- a master that is reporting and lists the token → **held**;
- a master that knows the holder process, reported after the lock's last
  heartbeat, and does not list it → **dead**, at once (a worker killed inside a
  job that is otherwise alive, or a master that ended);
- nobody to ask → the scheduler and the pid, as before, and after them the
  heartbeat's age: **stale** past `stale_after`, **unknown** before.

A lock is removed only on a positive answer that its holder is gone.

**When a job starts** it makes one pass over the locks before it builds its
queue: dead ones are removed and their keys queued, held ones are kept out of
the queue. The totals go to the event log (`locks_reconciled`) and the status:

```
  locks    260 held by 3 job(s), 17 reaped (4 dead job(s)), 0 stale, 0 unknown
```

**At any time**, from a login node:

```sh
bin/sweeprunner locks out/campaign
```

```
locks: 260 held by 3 job(s), 17 dead (4 dead job(s)), 0 stale, 0 unknown
  dead    pm/phase1/N=8_J=1.0/sample_001.running  c014:5512 job 3087801  heartbeat 28911 s ago  (holder is gone (scheduler / pid))
  held    pm/phase1/N=8_J=0.5/sample_001.running  c031:7120 job 3087883  heartbeat 41 s ago  progress 2630 s ago  (master c001_41233 has it out)
```

`heartbeat` fresh with `progress` old is a computation that is alive and not
advancing (the heartbeat is written by a child process; progress is written by
the computation, see [`report_progress`](@ref SweepRunner.report_progress)).

From Julia: `SweepRunner.locks(vault, keys)` (or `SweepRunner.locks(vault)`,
`SweepRunner.locks(outdir)`; the name is not exported) returns
[`LockInfo`](@ref SweepRunner.LockInfo) records and removes nothing;
`reap_dead_locks!(vault, keys)` removes the dead ones without running anything.
Without `keys` — `reap_dead_locks!(vault)`, `reap_dead_locks!(outdir)`, or from a
shell `sweeprunner locks <outdir> --reap` — it does so for every lock under the
vault, also on keys no current run lists. Only locks judged **dead** are
removed; stale and unknown ones are left to `stale_after`.

**Alive but not advancing.** `RunOpts(stuck_after = 1800)` turns an old progress
stamp into a verdict: a running key that has reported nothing for that long gets
a `key_stuck` warning in the event log (once), a `stuck` mark on its worker's
row in `sweeprunner status --workers`, and a line in the warnings. Nothing is
cut; `control!(…, :stop; keys = …, grace = …)` is how you act on it. It needs a
`work_fn` that reports (`report_progress` or `save_checkpoint!`); set it above
the longest step between two reports. Off by default.

**When a master leaves** it releases the locks it still has out — at the end
of a round there are none, on an exception or the scheduler's SIGTERM there
can be — and logs each key that was cut (`lock_released`, `why=master_exit`).
Orphans are then what a `kill -9` or a lost node leaves, not what every wall
clock leaves.

## 11. Changing a sweep while it runs

A request is one small file under the sweep's state directory. Every master
running on that `(project, run)` picks it up within `RunOpts.control_interval`
seconds (10 by default), applies it, acknowledges it, and logs who asked.

```sh
bin/sweeprunner pause      out/campaign
bin/sweeprunner resume     out/campaign
bin/sweeprunner cancel     out/campaign --select system.N=64,128        # drop queued units
bin/sweeprunner stop       out/campaign --select study=fdtx --grace 600 # stop running ones too
bin/sweeprunner stop       out/campaign --node c014 --grace 300         # everything on a node
bin/sweeprunner stop       out/campaign --grace 900                     # everything; masters return
bin/sweeprunner prioritise out/campaign --select system.N=16
bin/sweeprunner resize     out/campaign --n 1200
bin/sweeprunner drain      out/campaign --node c014
bin/sweeprunner enqueue    out/campaign --run phase2 --config configs/more_samples.toml
```

`--project` / `--run` limit a request to one `(project, run)`, `--master` to
one master (its id or its scheduler job id). From Julia it is
`SweepRunner.control!(vault, :cancel; select = Dict("system.N" => [64, 128]))`,
which returns the request id; `read_acks(vault, id)` says what each master did
with it.

| request | what the master does |
| :-- | :-- |
| `enqueue` | adds the keys to its queue (settling the ones already done) and keeps them for its later rounds |
| `cancel` | drops queued keys matching the filter, for the rest of the job; `--running` also stops the running ones |
| `stop` | running units in scope leave at their next safe point; queued ones in scope are not started; with no scope the master returns (`stopped_by = :request`) |
| `prioritise` | moves matching queued keys to the front |
| `resize` | retires workers down to `n` (idle first, each between units), or calls the `spawn` hook given to `run!` to start more |
| `drain` | stops dispatching to the workers on a node |
| `pause` / `resume` | no new dispatch; running units continue |

A master applies only requests made after it started, so a `stop` from last
week does not stop today's job. For the same reason a request sent when no
master is running is applied by nobody: the CLI then exits **3** and says so.
`--wait SECONDS` waits for a master's acknowledgement and prints what it did
(exit 4: nobody acknowledged in time; exit 5: a master could not apply it, for
example a `resize` with no worker pool). From Julia, `wait_acks(vault, id)`
does the same and `masters_listening(vault)` says who would read a request
sent now.

A request file that cannot be read is tried again on the next polls and, if it
stays unreadable, reported (`control_bad_request`) and acknowledged with the
error rather than dropped; a request a master cannot carry out is a
`control_not_applied` warning.

### Giving a stop a bound

A stop — the flag file, the deadline, or a `stop` request — used to be read
between keys only, so a key already in `work_fn` ran to its end. A `work_fn`
that can leave part-way says where:

```julia
function work_fn(key)
    p = SweepRunner.resume_point()
    for seg in (p === nothing ? 1 : p.step + 1):nseg
        run_segment!(key, seg)
        SweepRunner.report_progress(seg; of=nseg)
        SweepRunner.stop_point()        # leaves here if told to stop
    end
    return collect_result(key)
end
```

[`stop_point`](@ref SweepRunner.stop_point) throws `StopRequested`, which
`run!` takes as "stopped where it was told to": no attempt is spent, the lock
is released, and the next job resumes from the progress recorded.
[`should_stop`](@ref SweepRunner.should_stop) is the same question without the
throw. Both look at the filesystem at most once every `poll` seconds.

A unit that never reaches a safe point is bounded by `--grace`: once it has
passed, the master **removes the unit's worker and then releases its lock**
(`key_cut` in the event log). In that order: with the lock released first, the
key would be taken by another master while the old worker went on writing its
checkpoint over the new owner's. With the worker gone the round returns, so a
six-hour unit cannot hold a job that was told to stop. The worker is lost for
the rest of the job (a pool starts another when the queue needs one).

The job's own stop gets the same bound with `RunOpts(stop_grace = seconds)`:
once `stop_flag` is raised or the `deadline` has passed, units still running
after `stop_grace` are cut. It is `Inf` by default — a deadline set hours
ahead to let long keys finish is not turned into a kill — so set it below the
lead your scheduler gives before it kills the job.

Independently of any cut, a unit that no longer holds its key cannot write
over the one who does: `report_progress` returns `false` and
`save_checkpoint!` throws `StopRequested`, so the unit leaves.

### Workers that arrive late

At the same cadence the master adopts workers that joined since the round
began (`workers_joined`), after loading the modules named by `load=` on them.
A pool that is still ramping up when `run!` is called is used as it arrives,
not from the next round.

### Limits

- On the sequential path (no workers) requests are read between keys. The
  running key sees a stop through `should_stop`, but cannot be cut: the master
  is the process inside it.
- A cut needs a handle on what launched the worker (local workers and the
  pool's job steps have one). A worker started by a cluster manager that keeps
  none is asked to leave and may not; `key_cut` then says
  `worker_removed = false`, and the owner checks above are what protects the
  key's files.
- Enqueued keys run under the stage's `work_fn`; a prerequisite stage is not
  re-run for them.
- Growing the pool needs a `spawn` hook: `run!(…; spawn = n -> addprocs(…))`.

## 12. One file for a campaign

A campaign is many stages of many studies. Which run, in what order and under
which filters is one file:

```toml
[campaign]
name   = "2026-09"
outdir = "out/campaign"

[[study]]
name   = "conv"
stages = { phase1 = "conv_phase1.toml", phase2 = "conv_phase2.toml" }

[[study]]
name     = "fdtx"
stages   = { phase1 = "fdtx_phase1.toml", phase2 = "fdtx_phase2.toml", phase3 = "fdtx_phase3.toml" }
priority = 10                       # ahead of the others, with what it needs

[[study]]
name    = "typx"
stages  = { phase2 = "typx_phase2.toml" }
needs   = ["conv.phase1", "fdtx.phase1"]
enabled = true

[profile.short]                     # what a 30-minute job may take
max_key_time = "20min"
skip_stages  = ["phase1"]

[profile.large]
min_nodes = 16
```

- Config paths are relative to the meta file (or `[campaign] config_dir`).
- A job the controller submitted runs the profile named in `SWEEPRUNNER_PROFILE`
  (set for it at submission); `run_campaign!(…; profile = …)` overrides it, and
  `campaign_start` records which and where it came from. `min_nodes` says what
  size of job a profile is for: a smaller allocation running it is logged
  (`profile_too_small`), not refused.
- A stage needs the stage before it in its study (`chain = false` turns that
  off). `needs` on a study is what its first stage needs from other studies:
  `"study.stage"`, or `"study"` for all of that study's stages. Stages written
  as an array of tables (`[{name=…, config=…, needs=[…]}]`) run as written and
  can carry their own `needs`.
- Anything else in a study's or a profile's table is kept in `extra` and
  handed to the application.

The application's half is one function: how to open a stage.

```julia
campaign = SweepRunner.load_campaign("configs/campaign.toml")

open_stage(stage) = (;
    work_fn = WORK[stage.name],                 # required
    # vault = DataVault.Vault(stage.config; run=stage.name, outdir=campaign.outdir),  (default)
    # keys  = ParamIO.expand(vault.spec),                                             (default)
    # load = MyModel, affinity = …, prerequisite = …                                  (optional)
)

SweepRunner.run_campaign!(
    open_stage, campaign;
    profile = get(ENV, "SWEEP_PROFILE", nothing),
    cost    = (stage, key) -> estimated_seconds(stage, key),   # needed by max_key_time
)
```

What a job ran is on record: `events_campaign_<host>_<pid>.jsonl` under the
outdir has the meta file, its sha256, the profile and the stages in order,
then one line per stage.

**Check before submitting.** `bin/sweeprunner campaign configs/campaign.toml
--profile short` prints the studies, the validation report and the plan, and
exits non-zero when the file is not launchable: a config that is missing or
does not load, a `needs` that names nothing, a cycle, or two stages with
different configs that write the same project and run.

**Order.** Every stage comes after the stages it needs; among the ready ones
the highest `priority` goes first, and a stage inherits the priority of what
needs it. A stage whose needs are not complete is not started (the result says
which need), so a profile that skips `phase1` simply does not run the `phase2`
whose `phase1` is not there yet.

**Changing a running campaign.** The meta file is re-read between stages. Set
`enabled = false` or raise a `priority`, and the stages not yet started are
re-planned (`campaign_reloaded`); an edit that breaks the file is ignored and
said so once (`campaign_reload_refused`). Within a stage, the control channel
(guide 11) does the same job per key.

**What is left.** `SweepRunner.remaining_work(open_stage, campaign; profile,
cost)` returns, per stage, how many keys are undone, how many of them the
profile lets a job take, their estimated cost and the longest one, and which
needs block the stage.

## 13. Letting what is left decide the submissions

How many jobs to submit, where, and whether to resubmit used to be decided by
shell loops that did not know what was left. The pieces here do, and they are
explicit about it: **nothing is submitted unless the policy says
`dry_run = false`**, every decision is logged with its reason, and the budget
is a refusal.

The policy lives in the campaign's meta file:

```toml
[jobs]
name              = "ft"          # prefix of every job it submits
budget_node_hours = 5000          # hard: no default
max_jobs          = 8
dry_run           = true
default_key_time  = "10min"       # used when there is no cost model

[[jobs.partition]]
name           = "i8cpu"
nodes          = 8
time_limit     = "30min"
script         = "batch/run_campaign.sh"
profile        = "short"          # the campaign profile such a job runs
max_jobs       = 1
slots_per_node = 32               # workers per node: how much a job can take

[[jobs.partition]]
name           = "F16cpu"
nodes          = 16
time_limit     = "24h"
script         = "batch/run_campaign.sh"
profile        = "large"
max_jobs       = 6
slots_per_node = 32
```

```sh
bin/sweeprunner jobs configs/campaign.toml            # decide, print, submit nothing
bin/sweeprunner jobs configs/campaign.toml --submit   # submits, if the file says dry_run = false
bin/sweeprunner jobs configs/campaign.toml --submit --loop 300   # every 5 min until nothing is left
```

```
budget   812.0 used + 384.0 committed of 5000.0 node-hours
  hold   i8cpu       nothing runnable under profile short
  submit F16cpu      1930 unit(s) runnable under profile large, about 5120.4 worker-hours; 2 job(s) there
  refuse F72cpu      budget: 4871.0 node-hours used or committed, this job needs 1728.0, the budget is 5000.0
```

Per partition, [`decide`](@ref SweepRunner.decide):

- asks what is runnable under that partition's profile
  ([`remaining_work`](@ref SweepRunner.remaining_work): undone keys the profile
  lets a job take, in stages whose needs are complete). **Nothing runnable →
  nothing submitted** — the loop that kept a short queue busy after the
  eligible work ran out is this rule missing;
- counts what the jobs already pending or running there can still take;
- submits as many jobs as the rest needs, never more worker slots than units,
  within `max_jobs`;
- refuses a submission that would take used + committed node-hours past the
  budget. The account is the ledger (`<outdir>/sweeprunner/jobs/ledger.json`),
  so it holds across restarts.

What keeps that check from passing on an under-count:

- A submission is written to the ledger, and the ledger saved, **before**
  `sbatch` is called. If `sbatch` fails or times out, the row stays committed
  until a later poll finds a job of that name or it has been absent three
  polls: a job that was queued but not recorded is one a budget cannot see.
- A job counts as ended only after it has been absent from the scheduler's
  answer **three polls in a row**, and is then billed for what it can have run
  since it was last seen (up to its time limit), not for the last elapsed time
  read. A job in any listed state (`CONFIGURING`, `COMPLETING`, `SUSPENDED`, …)
  exists. A job that reappears is live again.
- A scheduler answer that cannot be trusted submits nothing: `squeue` failing,
  a line or a time that cannot be read, or an empty answer while the ledger
  holds live jobs. The refusal says why.
- The policy constructors reject values that would switch the check off (a NaN
  budget or time limit, zero or negative nodes).
- "Our jobs" are the ledger's ids plus the exact names `<name>-<partition>`;
  a policy named `ft` does not claim `ft2-…`.

What it still cannot see: jobs submitted outside the controller, and a ledger
file that was deleted (it reads as nothing used). Nothing reconciles with
`sacct`.

The batch script is yours; it receives `SWEEPRUNNER_PROFILE` and whatever the
partition's `env` names.

From Julia, with a cost model:

```julia
campaign = SweepRunner.load_campaign("configs/campaign.toml")
policy   = SweepRunner.load_job_policy("configs/campaign.toml")
ctl      = SweepRunner.JobController(SweepRunner.SlurmScheduler(), policy, campaign.outdir)
work     = SweepRunner.campaign_work(open_stage, campaign; cost = (stage, key) -> seconds(stage, key))

SweepRunner.manage!(ctl, work)                       # one round
SweepRunner.controller_loop!(ctl, work; interval=300) # until nothing is left
```

`manage!` is also what a job calls as its last act to resubmit **only if work
remains**, instead of a chain script with a fixed number of generations.

### Leaving on purpose

Inside a job, the master can give the allocation back instead of holding it to
the wall clock for a few long units:

```julia
RunOpts(min_busy_fraction = 0.25, idle_grace = 900)
```

When the queue is empty and fewer than a quarter of the workers have had a
unit for 15 minutes, the master stops (`underused` in the event log,
`stopped_by = :underused`). The units still running are told to stop and leave
at their next [`stop_point`](@ref SweepRunner.stop_point) with their progress
recorded, so the next job — sized to what is left — resumes them.

Where the scheduler can shrink a job, `SweepRunner.shrink(scheduler, jobid,
nodes)` gives nodes back; send a `drain` request for those nodes first so
nothing is dispatched to them.

### The scheduler is behind an interface

`submit / cancel / job_states / remaining_time / shrink` on a
[`Scheduler`](@ref SweepRunner.Scheduler). `SlurmScheduler` is the first
backend; `MockScheduler` is what the policy's tests run against, and what you
can try a policy on before it touches a queue.

## 14. What a key cost

Every finished key leaves a `key_done` record with its wall time, CPU time
(all threads), the cores its worker had, its peak resident memory, the node it
ran on and its class:

```julia
run_loop!(work_fn, vault, keys; key_class = k -> "N=$(k.params["system.N"])")
```

Inside `work_fn`, `SweepRunner.note_key!(segments = nseg, chi = chi)` adds
fields to the record.

```sh
bin/sweeprunner costs out/campaign
```

```
class                           keys  median s     p90 s  cores  used  peak GB
N=16                            4120     212.4     388.0      1   97%     1.84
N=32                            1890    3310.9    5120.2      4   39%     6.10
N=64                             212   61804.0   94310.5      7   55%    11.72
```

`used` is CPU time over wall time over cores: 39% at 4 cores is the number
that says a key class would run more work per node-hour on fewer threads.

`run_loop!` writes the same table to `<state_root>/costs.json` when it ends,
so the next job reads measurements instead of a hand-fitted formula:

```julia
table = SweepRunner.load_cost_table(vault)
class = k -> "N=$(k.params["system.N"])"
secs  = SweepRunner.measured_cost(table, class; fallback = k -> formula(k))   # key -> seconds
bytes = SweepRunner.measured_mem(table, class; margin = 1.2, fallback = k -> declared(k))

SweepRunner.run_campaign!(open_stage, campaign; profile = "short",
                          cost = (stage, key) -> secs(key))
```

A class the table has not seen falls back to the application's estimate, so
the first job of a new study still has one.

**It is the default.** With a `key_class`, `run!` loads the stage's table
itself and uses it as `cost`, falling back to the `cost` you passed for the
classes it has not seen; a `cost_source` event says how many of the round's
keys were measured and how many fell back.

**Every attempt is on record**, not only the one that finished: an attempt
that failed, was stopped or cut, lost its lock, or whose worker died leaves a
`key_spent` record with its outcome. A key's time in the table is the **sum
over its attempts** — a key that ran six hours over four jobs took six hours,
not the forty minutes of its last leg — and `unfinished` counts the keys of a
class that have attempts and no finish (the ones too big for their request).

**Unknown is not zero.** `SweepRunner.key_seconds(hook, key)` is how the run,
the campaign and the job controller ask a cost hook: `nothing` when it throws
or answers something that is not a finite, non-negative number. With a
`deadline`, a key your hook has no answer for is not assumed to fit: it is
held back and counted (`held_back`, with `cost_unknown`). A class missing from
a table this package loaded by itself is different — it has to run once to be
measured, so it runs. Under a profile's `max_key_time`, a key of unknown cost
is not eligible. `key_costs(vault)` returns the raw
records (`KeyCost`) and `cost_summary(costs; by = c -> c.host)` groups them
any way you like — per node, per thread count.

The peak is counted from the start of the key on Linux (the kernel's
high-water mark is reset), so it is the key's and not the largest key that
worker ever ran. Where the reset is not available the record says
`rss_scope = "process"`, and the class's summary `rss_process = true`.

The table is rewritten with the manifest while a round runs
(`RunOpts.manifest_interval`) and when `run_loop!` ends, so a job killed at
its wall clock leaves what it measured.

## 15. Where a job's core-hours went

A job is charged nodes × elapsed time. The master keeps an account of it as it
dispatches, in core-seconds, and writes it to the event log when it ends
(`job_account`) and to its status while it runs:

```sh
bin/sweeprunner account out/campaign
```

```
phase1  c001_41233 job 3087883  ended  0.83 h
allocated         7632.0 core-h
computing         2890.0 core-h (38%)
  kept            2410.0 core-h (32%)
  lost             480.0 core-h (6%)   217 key(s) cut
start-up           410.0 core-h (5%)
never started     3960.0 core-h (52%)
idle               350.0 core-h (5%)
  lock_busy         60.0 core-h (1%)
  queue_empty      290.0 core-h (4%)
other               22.0 core-h (0%)
```

| line | what it is | measured or estimated |
| :-- | :-- | :-- |
| `computing` | a worker had a key | measured at each dispatch |
| `kept` | the key finished, or the part of it up to its last `report_progress` | measured |
| `lost` | the part after the last progress stamp of a key that was cut, stopped, failed, or whose worker died | measured |
| `start-up` | each worker's cores from the master's start to that worker's first key | measured |
| `never started` | (workers planned − joined) × mean cores × elapsed | estimated; needs `note_workers!(planned=…)` |
| `idle` | a worker that had already had a key had none, by reason: `queue_empty`, `lock_busy`, `paused`, `stopping` | measured |
| `other` | the rest of the allocation: the master, cores no worker was given | by subtraction |

`lost` is the number the checkpoint interval is tuned against: it is what
every job end, cancel and out-of-memory kill threw away. A `work_fn` that
reports progress more often loses less; one that never reports loses the whole
attempt.

`SweepRunner.account_snapshot(master)` returns the same numbers as a `Dict`;
`read_status(vault)[i]["account"]` reads them from outside while the job runs.

## 16. Checkpoints inside a key

Every job end — wall clock, cancel, a lost node, an out-of-memory kill —
throws away what each running key did since it last saved. If saving is left
to each `work_fn`, that is up to a whole segment per key, per job, and every
application solves it again. The pipeline does it:

```julia
function work_fn(key)
    cp    = SweepRunner.checkpoint()
    state = something(SweepRunner.load_checkpoint(cp), initial_state(key))
    while !finished(state)
        state = advance(state)                       # one step
        if SweepRunner.checkpoint_due(cp)
            SweepRunner.save_checkpoint!(cp, state; step = state.step, of = nsteps)
        end
        SweepRunner.stop_point()                     # leave here if told to stop
    end
    return result(state)
end
```

- `load_checkpoint(cp)` is the state the last attempt saved — on any worker,
  of any job — or `nothing`.
- `checkpoint_due(cp)` is the one question the loop asks. It is true every
  `RunOpts.checkpoint_every` seconds (600 by default), **at once when the unit
  has been told to stop** (the flag, the deadline, a `stop` request), and once
  when the job's deadline comes within a minute. The application does not
  implement the timing.
- `save_checkpoint!(cp, state; step, of)` writes atomically, replaces the
  previous checkpoint, and stamps the progress — which is what `sweeprunner
  status` and `locks` show as "advancing", and what the account counts as
  `kept` when the key is cut.
- When the key finishes, its checkpoint and progress stamp are removed.

Put `stop_point()` after the save, so a stop always leaves from a state that
is on disk. What a key can lose is the work since its last save: with
`checkpoint_every = 600`, up to 600 s. `checkpoint_due` is also true at once on
a stop and within a minute of `opts.deadline`, so a job that is given either
before its wall clock saves on the way out; a job killed with neither loses up
to `checkpoint_every` per running key.

`state` is saved with JLD2, so it can be any Julia value JLD2 can write.

### Does it really resume?

A result that depends on where the key was interrupted is a wrong result that
only shows up on a cluster. Test it on a laptop:

```julia
r = SweepRunner.check_checkpoints(work_fn, scratch_vault, key)
@test r.same             # identical to an uninterrupted run
@test r.restarts > 0     # it did take checkpoints
```

`check_checkpoints` runs the key once straight through and once cut
immediately after **every** `save_checkpoint!` and restarted from it, and
compares the two results.

## 17. Several masters on one sweep, and keys of very different length

### Sharding the queue

Several jobs running one sweep each build the same queue in the same order
and start from its head, so they spend their first passes on each other's
locks. Tell each which share to start on:

```julia
RunOpts(shard = (i, m))        # master i of m, 0 <= i < m
```

or set `SWEEPRUNNER_SHARD=i/m` in the batch script; a Slurm array task gets
its share from the array on its own. A master draws the keys whose hash falls
in its share first and the others' after, so **every master still covers every
key** — a share is where it starts. The test suite runs two masters on 24
keys both ways and requires the sharded pair to collide no more than the
unsharded one; how much less depends on timing, and it logs both counts.

`run!` returns `collisions` (keys handed to a worker that came back because
another master had taken them), and `stage_done` logs it, so the cost is
visible per job.

### Cost and the wall clock

```julia
run_loop!(work_fn, vault, keys;
    opts     = RunOpts(deadline = job_end - 120, order = :longest_first),
    cost     = SweepRunner.measured_cost(table, class; fallback = formula),  # key -> seconds
    min_time = key -> seconds_to_next_checkpoint(key),
)
```

- **`order = :longest_first`** draws the keys with the largest `cost` first:
  the long keys start while there is time for them, and the short ones fill
  what is left.
- **A key that cannot get anywhere is not started.** With a `deadline`, a key
  whose `min_time` (how long it needs to reach its next checkpoint; `cost`
  when not given) exceeds the time left is passed over, counted in
  `held_back` and logged once. The check is made at each hand-out, so as the
  job runs down the long keys drop out and the short ones still run. This
  replaces per-partition filters by hand: the keys that occupied half the
  workers of every short job without advancing are the ones held back.
- `run_loop!` returns at once (`stopped_by = :deadline`) when all that is left
  was held back, rather than sitting out idle rounds.
- With `RunOpts(min_busy_fraction = …)` (guide 13) the master then leaves when
  what is running is too little to hold the allocation for.

A `work_fn` that keeps a checkpoint (guide 16) needs only `checkpoint_every`
seconds to get somewhere, so `min_time = _ -> opts.checkpoint_every` makes
every key fit almost any job.

## 18. Start-up, and what the master carries

**Leave in seconds when there is nothing to do.** Ask before starting a worker:

```julia
SweepRunner.todo_count(vault, keys) == 0 && exit(0)
SweepRunner.init_workers!()
```

**Start workers from a system image.** `init_workers!(sysimage = path)`, or
`SWEEPRUNNER_SYSIMAGE`, starts every worker with `--sysimage`: a worker is up
in seconds instead of loading and compiling the application, which matters
most when workers are started all job long. Building the image
(PackageCompiler, once per commit, on a login node) is the application's
step, and the master should run from the same image.

**A limit per master that is a message.** Under `:slurm` every worker is an
`srun` client on the master's node and takes about seven ports of the
cluster's `SrunPortRange`. Past the range, workers neither join nor fail: a
72-node job planned 3735 workers, stopped at 1782 without an error, and ran at
48% of its cores to the end. `init_workers!` now refuses to start more than
`max_workers` — `SWEEPRUNNER_MAX_WORKERS`, else `srun_worker_limit()` read from
`scontrol show config` (1607 on that cluster) — and says how many masters it
would take.

**Several masters in one allocation.** Above the limit, cut the allocation
into node groups and run one master per group, each on its own share of the
keys:

```julia
groups = SweepRunner.split_nodes(ENV["SLURM_JOB_NODELIST"], m)
```

```sh
for i in $(seq 0 $((m-1))); do
  srun -N ${n[$i]} -w ${group[$i]} --export=ALL,SWEEPRUNNER_SHARD=$i/$m \
       julia run_campaign.jl &
done; wait
```

Start-up rate and every per-master limit then scale with the allocation. The
masters need no broker: sharding (guide 17) keeps them off each other's keys,
the locks and the status files do the rest.

**Workers keep their own logs.** `init_workers!(worker_logs = dir)` (or
`worker_logs!(dir)` once workers exist) makes each worker write stdout and
stderr to `<dir>/worker_<host>_<pid>.log`, instead of relaying every line
through the master to be printed with a `From worker N:` prefix.

**The round's context travels once.** The work function, the vault, the log
and the options are installed on a worker when its dispatch loop starts; each
key then carries only itself, its lock token and its resume point.

**The manifest is kept while the round runs** (`RunOpts.manifest_interval`,
300 s). It used to be written only when a round ended, so a job killed at its
wall clock left none of its completions in it and the next job found them
again one marker at a time.

**Where the round's time went.** `stage_done` carries `prepare_secs` (loading
modules on workers, source observation), `scan_secs` (the pass over markers
and locks), `dispatch_secs`, `manifest_secs` and `total_secs`.

## 19. Workers sized to their keys

`init_workers!` starts `n` identical workers, so a sweep whose keys differ in
size sizes every worker for its largest key. A pool sizes each worker to the
key it will run:

```julia
pool = SweepRunner.SizedPool(;                     # SlurmStepSpawner in a job, LocalSpawner outside
    key_req = k -> KeyReq(k.params["resources.cores"], mem_gb(k)),
)
run_loop!(work_fn, vault, keys; pool = pool, load = MyModel)    # no init_workers!
```

- The pool starts a worker of the size a queued key needs, on the node that
  keeps the most memory free after it, and a worker only takes keys it can
  hold. Under Slurm each worker is its own job step (`srun --exact`
  `--cpus-per-task` `--mem`), so its memory limit is its own.
- **Backfill**: a smaller key starts past a larger one that fits nowhere yet,
  until the larger has waited `starve_after` (600 s); from then on nothing is
  started ahead of it, and the room it needs is freed by keys finishing.
- **Retire**: an idle worker whose size no queued key fits gives its room back
  after `retire_after` (120 s) when another size is waiting.
- **Out of memory**: a worker that dies under a key has the key retried with
  `mem_growth` (1.5×) the memory, up to what a node has (`pool_retry_mem`).
  A first estimate that is too small costs a retry, not the node.
- A key that needs more than any node offers is reported once
  (`key_too_big`) and not retried forever.
- `measured_mem(table, class; margin, fallback)` (guide 14) is a `key_req`
  memory from what keys of that class actually peaked at.

- The pool's workers are removed when the `run!` / `run_loop!` it was given
  to returns (`keep = true` keeps them; then call `SweepRunner.shutdown!`).
- The pool follows requests to the master (guide 11): nothing is started
  while it is paused or stopping, a drained node gets no new worker, and a
  `resize` target caps the pool.

### Limits and failures that are said

- **The per-master limit.** Under Slurm every worker is an `srun` client on
  the master's node. The pool holds at most `max_workers`:
  `SWEEPRUNNER_MAX_WORKERS`, else the cluster's `SrunPortRange`, else 1500
  when that cannot be read — an unknown limit is a cap, not no cap. Which
  one it used is logged (`pool_limit`), and reaching it is logged once
  (`pool_at_limit`). Past it, run several masters on node groups:
  `SWEEPRUNNER_NODELIST=<group>` (and `SWEEPRUNNER_MEM_PER_NODE_MB=<the
  allocation's per-node memory>` for a master that is itself a job step),
  with `SWEEPRUNNER_SHARD=i/m`.
- A start that fails is logged (`pool_spawn_failed`), one that brings fewer
  workers than asked too (`pool_spawn_short`), and a worker that started but
  could not be readied is removed. Starts that neither join nor fail for
  `stall_after` are a `pool_stalled` warning. **Ten failed starts in a row
  with keys still queued is an error from `run!`** (`pool_gave_up`), not a
  round that quietly ended.
- A worker whose launching process has exited is treated as gone even before
  Distributed notices the connection drop.

### How many threads

A worker is started with more cores than its key declares when that is what
its size stands for, by policy:

| `threads` | cores given | for |
| :-- | :-- | :-- |
| `:throughput` (default) | the cores its memory share stands for on that node (`mem × free cores / free memory`), at most `max_threads` | charged or scarce cores: the most work per node-hour. Where memory binds before cores, a key sized by its declared cores alone strands the rest of the node |
| `:fastest` | for a key that declares more than one core: up to `max_threads`, with the memory that comes with them (a one-core key stays as under `:throughput`) | free or abundant cores, or a key that has to finish |
| `:finish_by` | as `:throughput`, but a key that would not reach its next checkpoint before the job's `deadline` gets the cores that get it there | a deadline |

`:finish_by` needs to know how a key scales: `speedup = (key, cores) -> factor`.
Measure it rather than assume it — run a class at more than one thread count
and build the curve from the cost records:

```julia
sp = SweepRunner.measured_speedup(SweepRunner.key_costs(vault), class)
pool = SweepRunner.SizedPool(; key_req, threads = :finish_by, speedup = sp, max_threads = 12)
```

A class measured at one thread count only gets `1.0`: no claim that threads
help. (Downstream, 4 threads made a step 1.55× faster — 39% efficiency — so
`:throughput` is the default.)

### What has and has not been run

The pool, the planner (`plan_spawns`, a pure function you can call with your
own node list to see what it would start) and `LocalSpawner` are covered by
the test suite with real local workers. `SlurmStepSpawner` and its
`StepManager` are the downstream implementation moved here; reading the
allocation and building the `srun` line are tested, starting job steps is not
— that needs an allocation.
