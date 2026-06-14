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
2. Half-written payload files do not exist — [`atomic_write`](@ref ParallelManager.atomic_write)
   renames only after `fsync`, so readers see either the previous version
   or the new one.
3. Start a new master with the same `run.jl`. After `opts.stale_after`
   seconds (default 600), the new master will see the abandoned locks as
   stale, reclaim them, and re-run the affected keys.

For tests, tighten the window:

```julia
ParallelManager.run!(work_fn, vault, keys;
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
ParallelManager.run!(work_fn, vault, keys;
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
ParallelManager.init_workers!(mode=:sequential, verbose=false)
ParallelManager.run!(work_fn, vault, keys)
```

or with multi-threading:

```bash
julia --project --threads=8 run.jl
```

`init_workers!(mode=:auto)` will pick `:threads` based on
`Threads.nthreads() > 1`.

## 8. Cleaning a corrupted manifest

A corrupted `manifest.jld2` is treated as empty by
[`load_manifest`](@ref ParallelManager.load_manifest), so the worst case
is a full re-run (made safe by per-key locks + `is_done` re-check). If
you want to force that:

```bash
rm out/manifest/<project>/<run>/manifest.jld2
```

Per-key `.done` files written by `DataVault.mark_done!` are the
authoritative source of truth — the manifest is just a cache.
