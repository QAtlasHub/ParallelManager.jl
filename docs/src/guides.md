# Guides

Recipes for common situations.

## 1. Analyzing the event log

`events.jsonl` is one JSON object per line. Any JSONL-aware tool works.

With `jq`:

```bash
# How many keys completed this session?
jq -c 'select(.kind == "key_done")' out/events.jsonl | wc -l

# Slowest 10 keys by wall-clock:
jq -c 'select(.kind == "key_done") | {key, secs}' out/events.jsonl \
  | jq -s 'sort_by(-.secs) | .[:10]'

# Lock contention across all masters:
jq -c 'select(.kind == "lock_busy") | .key' out/events.jsonl | sort | uniq -c | sort -nr
```

With Julia + `DataFrames`:

```julia
using JSON3, DataFrames

rows = [JSON3.read(l) for l in readlines("out/events.jsonl")]
df = DataFrame(rows)
filter!(:kind => ==("key_done"), df)
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

1. Its lock directories remain on disk with stale `heartbeat` mtimes.
2. Half-written payload files do not exist — [`atomic_write`](@ref SweepRunner.atomic_write)
   renames only after `fsync`, so readers see either the previous version
   or the new one.
3. Start a new master with the same `run.jl`. After `opts.stale_after`
   seconds (default 600), the new master will see the abandoned locks as
   stale, reclaim them, and re-run the affected keys.

For tests, tighten the window:

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
                         stale_after=1800.0,          # 30 min
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
