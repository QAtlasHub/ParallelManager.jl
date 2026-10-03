# API reference

```@docs
SweepRunner
```

## AtomicIO

```@docs
SweepRunner.atomic_write
SweepRunner.atomic_touch
```

## EventLog

```@docs
SweepRunner.EventLog
SweepRunner.log_event
SweepRunner.merge_event_logs
```

## Manifest

```@docs
SweepRunner.Manifest
SweepRunner.manifest_path
SweepRunner.load_manifest
SweepRunner.save_manifest
SweepRunner.add_complete!
SweepRunner.is_complete
SweepRunner.todo_keys
SweepRunner.manifest_root
SweepRunner.merge_and_save_manifest!
```

## InitWorkers

```@docs
SweepRunner.init_workers!
SweepRunner.detect_mode
SweepRunner.verify_workers!
SweepRunner.srun_worker_limit
SweepRunner.split_nodes
SweepRunner.worker_logs!
```

## Run

```@docs
SweepRunner.RunOpts
SweepRunner.run!
SweepRunner.run_loop!
SweepRunner.todo_count
SweepRunner.key_seconds
```

## Artifacts

```@docs
SweepRunner.artifact_affinity
```

## Liveness

```@docs
SweepRunner.owner_token()
SweepRunner.owner_token(::AbstractString, ::Integer)
SweepRunner.holder_liveness
```

## Prerequisite

```@docs
SweepRunner.Prerequisite
SweepRunner.run_prerequisite!
```

## Preflight

```@docs
SweepRunner.Finding
SweepRunner.PreflightReport
SweepRunner.launchable
SweepRunner.n_errors
SweepRunner.n_warns
SweepRunner.check_injective!
SweepRunner.check_opens!
SweepRunner.representative_keys
SweepRunner.on_grid
```
