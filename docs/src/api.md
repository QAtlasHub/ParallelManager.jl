# API reference

```@docs
ParallelManager
```

## AtomicIO

```@docs
ParallelManager.atomic_write
ParallelManager.atomic_touch
```

## EventLog

```@docs
ParallelManager.EventLog
ParallelManager.log_event
ParallelManager.merge_event_logs
```

## Manifest

```@docs
ParallelManager.Manifest
ParallelManager.manifest_path
ParallelManager.load_manifest
ParallelManager.save_manifest
ParallelManager.add_complete!
ParallelManager.is_complete
ParallelManager.todo_keys
ParallelManager.manifest_root
ParallelManager.merge_and_save_manifest!
```

## InitWorkers

```@docs
ParallelManager.init_workers!
ParallelManager.detect_mode
ParallelManager.verify_workers!
```

## Run

```@docs
ParallelManager.RunOpts
ParallelManager.run!
ParallelManager.run_loop!
```
