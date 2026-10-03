# API reference: the control plane

What a running sweep can be asked and told, and what it records: the task table and progress,
status, locks, the control channel, checkpoints, the account and the cost records. The core
(`run!`, `RunOpts`, the manifest, the event log, workers) is in the [API reference](api.md);
the worker pool, campaigns and jobs are in [API: pool, campaigns, jobs](api_campaign.md).

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
SweepRunner.wait_acks
SweepRunner.masters_listening
SweepRunner.control_dir
SweepRunner.poll_control!
SweepRunner.ControlState
```

## Checkpoint

```@docs
SweepRunner.checkpoint
SweepRunner.Checkpoint
SweepRunner.load_checkpoint
SweepRunner.save_checkpoint!
SweepRunner.checkpoint_due
SweepRunner.check_checkpoints
SweepRunner.checkpoint_dir
```

## Account

```@docs
SweepRunner.Account
SweepRunner.account_snapshot
SweepRunner.print_account
```

## Cost

```@docs
SweepRunner.note_key!
SweepRunner.KeyCost
SweepRunner.key_costs
SweepRunner.cost_summary
SweepRunner.write_cost_table
SweepRunner.load_cost_table
SweepRunner.cost_table_path
SweepRunner.measured_cost
SweepRunner.measured_mem
SweepRunner.print_costs
```

