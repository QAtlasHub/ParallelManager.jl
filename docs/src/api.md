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
```

## Run

```@docs
SweepRunner.RunOpts
SweepRunner.run!
SweepRunner.run_loop!
```

## Task table

```@docs
SweepRunner.TaskTable
SweepRunner.TaskRow
SweepRunner.Progress
SweepRunner.next_task!
SweepRunner.start_task!
SweepRunner.settle!
SweepRunner.hold!
SweepRunner.requeue!
SweepRunner.add_tasks!
SweepRunner.settle_queued!
SweepRunner.task_counts
```

## Progress

```@docs
SweepRunner.report_progress
SweepRunner.resume_point
SweepRunner.read_progress
SweepRunner.progress_dir
```

## Master

```@docs
SweepRunner.Master
SweepRunner.state_root
```

## Status

```@docs
SweepRunner.read_status
SweepRunner.print_status
SweepRunner.note_workers!
SweepRunner.status_snapshot
SweepRunner.write_status
SweepRunner.status_path
SweepRunner.status_tick!
SweepRunner.WorkerSample
SweepRunner.expand_nodelist
SweepRunner.cli
```

## Locks

```@docs
SweepRunner.locks
SweepRunner.LockInfo
SweepRunner.judge_lock
SweepRunner.lock_summary
SweepRunner.print_locks
SweepRunner.reap_dead_locks!
```

## Control

```@docs
SweepRunner.control!
SweepRunner.should_stop
SweepRunner.stop_point
SweepRunner.StopRequested
SweepRunner.KeyFilter
SweepRunner.matches
SweepRunner.read_requests
SweepRunner.read_acks
SweepRunner.control_dir
SweepRunner.poll_control!
SweepRunner.ControlState
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
SweepRunner.check_injective!
SweepRunner.check_opens!
SweepRunner.representative_keys
SweepRunner.on_grid
```
