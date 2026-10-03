# API reference: pool, campaigns, jobs

Workers sized to their keys, the meta config of a campaign, and deciding submissions from what
is left. See also the [API reference](api.md) and [API: control plane](api_plane.md).

## Pool

```@docs
SweepRunner.SizedPool
SweepRunner.KeyReq
SweepRunner.PoolNode
SweepRunner.Spawner
SweepRunner.LocalSpawner
SweepRunner.SlurmStepSpawner
SweepRunner.StepManager
SweepRunner.default_spawner
SweepRunner.start_workers
SweepRunner.worker_size
SweepRunner.plan_spawns
SweepRunner.pool_summary
SweepRunner.shutdown!
SweepRunner.measured_speedup
```

## Campaign

```@docs
SweepRunner.Campaign
SweepRunner.StageSpec
SweepRunner.CampaignProfile
SweepRunner.stage_id
SweepRunner.load_campaign
SweepRunner.validate_campaign
SweepRunner.plan_campaign
SweepRunner.run_campaign!
SweepRunner.remaining_work
SweepRunner.parse_duration
```

## Jobs

```@docs
SweepRunner.Scheduler
SweepRunner.JobSpec
SweepRunner.JobState
SweepRunner.submit
SweepRunner.cancel
SweepRunner.job_states
SweepRunner.remaining_time
SweepRunner.shrink
SweepRunner.SlurmScheduler
SweepRunner.MockScheduler
SweepRunner.PartitionPolicy
SweepRunner.JobPolicy
SweepRunner.load_job_policy
SweepRunner.Ledger
SweepRunner.observe!
SweepRunner.record_intent!
SweepRunner.confirm_submit!
SweepRunner.node_hours
SweepRunner.Decision
SweepRunner.decide
SweepRunner.campaign_work
SweepRunner.JobController
SweepRunner.manage!
SweepRunner.controller_loop!
SweepRunner.print_decisions
```

