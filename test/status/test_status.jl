# Status (#63): what a running sweep is doing, written where it can be asked from outside.

using SweepRunner, Test, DataVault, ParamIO, JSON3, Distributed
using SweepRunner: Master, expand_nodelist

const _ST_CFG = joinpath(@__DIR__, "..", "run", "fixtures", "study.toml")

function _st_vault(f; run="st")
    outdir = mktempdir()
    try
        f(DataVault.Vault(_ST_CFG; run=run, outdir=outdir), outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

function _st_workers(f, n)
    nprocs() > 1 && rmprocs(workers())
    addprocs(n; exeflags="--project=$(dirname(Base.active_project()))")
    try
        @everywhere workers() Core.eval(
            Main, :(using SweepRunner, DataVault, ParamIO, Distributed)
        )
        f()
    finally
        rmprocs(workers())
        note_workers!(; planned=0, launched=0)
    end
end

function _st_events(outdir)
    logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir(outdir))
    return [JSON3.read(l) for f in logs for l in readlines(joinpath(outdir, f))]
end

@testset "expand_nodelist: the shapes SLURM_JOB_NODELIST takes" begin
    @test expand_nodelist("c001") == ["c001"]
    @test expand_nodelist("c[001-003]") == ["c001", "c002", "c003"]
    @test expand_nodelist("c[09-11,20],gpu01") == ["c09", "c10", "c11", "c20", "gpu01"]
    @test expand_nodelist("n[1-2]-ib,m7") == ["n1-ib", "n2-ib", "m7"]
    @test expand_nodelist("") == String[]
    # Not understood is empty, never a guess.
    @test expand_nodelist("c[1-2][a-b]") == String[]
    @test expand_nodelist("c[a-b]") == String[]
end

@testset "status: a finished sequential run! leaves its counts and says it ended" begin
    _st_vault() do v, outdir
        ks = DataVault.keys(v)
        r = run!(k -> Dict{String,Any}("x" => 1), v, ks)
        @test r.done == length(ks)
        st = only(read_status(v))
        @test st["state"] == "ended"
        @test st["stale"] == false
        @test st["stage"] == "st"
        @test st["tasks"]["total"] == length(ks)
        @test st["tasks"]["done"] == length(ks)
        @test st["tasks"]["todo"] == 0
        @test st["workers"]["joined"] == 1
        @test st["workers"]["busy"] == 0
        @test only(st["worker_table"])["pid"] == getpid()
        @test only(st["nodes"])["host"] == gethostname()
        # The same file, found from the outdir alone.
        @test only(read_status(outdir))["master"] == st["master"]
        out = sprint(io -> print_status(io, outdir; workers=true))
        @test occursin("tasks    total $(length(ks))  done $(length(ks))", out)
        @test occursin("(idle)", out)
        @test SweepRunner.cli(["status", outdir]; io=IOBuffer()) == 0
        js = sprint(io -> SweepRunner.cli(["status", outdir, "--json"]; io=io))
        @test JSON3.read(js)[1].state == "ended"
    end
end

@testset "status: status_interval=0 writes nothing" begin
    _st_vault() do v, outdir
        run!(
            k -> Dict{String,Any}("x" => 1),
            v,
            DataVault.keys(v);
            opts=RunOpts(; status_interval=0),
        )
        @test isempty(read_status(v))
        @test sprint(io -> print_status(io, outdir)) == "no status found\n"
    end
end

@testset "status: a master that stopped writing without ending is reported gone" begin
    _st_vault() do v, _
        run!(k -> Dict{String,Any}("x" => 1), v, DataVault.keys(v))
        path = only(read_status(v))["path"]
        d = JSON3.read(read(path, String), Dict{String,Any})
        d["state"] = "running"
        d["updated"] = time() - 10 * d["interval"]
        write(path, JSON3.write(d))
        st = only(read_status(v))
        @test st["stale"] == true
        @test occursin("GONE", sprint(io -> print_status(io, v)))
    end
end

@testset "status: utilisation is CPU time over wall time over cores" begin
    m = Master()
    m.who[7] = (; host="n1", pid=1, cores=4)
    SweepRunner._store_sample!(m, 7, (; cpu=10.0, wall=100.0, rss=5))
    @test isnan(m.samples[7].util)                      # one reading says nothing
    SweepRunner._store_sample!(m, 7, (; cpu=50.0, wall=110.0, rss=6))
    @test m.samples[7].util ≈ 1.0                       # 40 CPU-s in 10 s on 4 cores
    SweepRunner._store_sample!(m, 7, (; cpu=60.0, wall=120.0, rss=6))
    @test m.samples[7].util ≈ 0.25
    s = SweepRunner._sample()
    @test s.cpu > 0 && s.rss > 0
end

@testset "status: while it runs, each worker's key, lock and progress can be read" begin
    _st_workers(2) do
        _st_vault() do v, outdir
            ks = DataVault.keys(v)
            work = k -> begin
                SweepRunner.report_progress(1; of=2)
                sleep(2.0)
                return Dict{String,Any}("x" => 1)
            end
            t = @async run!(work, v, ks; opts=RunOpts(; status_interval=0.2))
            seen = nothing
            t0 = time()
            while !istaskdone(t) && time() - t0 < 120
                st = read_status(v)
                if !isempty(st) && st[1]["workers"]["busy"] == 2
                    rows = [r for r in st[1]["worker_table"] if haskey(r, "progress_step")]
                    if length(rows) == 2
                        seen = st[1]
                        break
                    end
                end
                sleep(0.1)
            end
            @test seen !== nothing
            if seen !== nothing
                @test seen["state"] == "running"
                @test seen["workers"]["joined"] == 2
                @test seen["workers"]["idle"] == 0
                @test seen["tasks"]["running"] == 2
                kset = Set(ParamIO.canonical.(ks))
                for row in seen["worker_table"]
                    @test row["key"] in kset
                    @test row["progress_step"] == 1
                    # The lock on disk is the one the master named.
                    k = ks[findfirst(x -> ParamIO.canonical(x) == row["key"], ks)]
                    @test DataVault.running_owner(v, k) == row["owner"]
                    @test startswith(
                        row["owner"], string(row["host"], ":", row["pid"], ":")
                    )
                end
                @test only(seen["nodes"])["busy"] == 2
            end
            r = fetch(t)
            @test r.done == length(ks)
            last = only(read_status(v))
            @test last["state"] == "ended"
            @test last["tasks"]["done"] == length(ks)
            # Readings were taken: the workers slept, so they used little of their cores.
            @test all(r -> haskey(r, "rss") && r["rss"] > 0, last["worker_table"])
        end
    end
end

@testset "status: a running key with no progress for stuck_after is said, once (#112)" begin
    @test_throws ArgumentError RunOpts(; stuck_after=-1)
    _st_workers(2) do
        _st_vault() do v, outdir
            ks = DataVault.keys(v)[1:2]
            advancing = ParamIO.canonical(ks[1])
            gate = joinpath(outdir, "gate")
            # Both keys run until the test opens the gate. One keeps reporting; the other is
            # alive and says nothing. The threshold is 25 reports wide, so a runner that stalls
            # for a second or two does not make the healthy key look stuck.
            work =
                k -> begin
                    step = 0
                    t0 = time()
                    while !isfile(gate) && time() - t0 < 300
                        step += 1
                        ParamIO.canonical(k) == advancing &&
                            SweepRunner.report_progress(step)
                        sleep(0.2)
                    end
                    return Dict{String,Any}("x" => 1)
                end
            # Once through the pipeline on other keys first: a worker's first key spends
            # seconds compiling before its first report — on a loaded runner more than the
            # threshold — and that is no progress either, but not what this is about.
            warm =
                k ->
                    (SweepRunner.report_progress(1); sleep(0.5); Dict{String,Any}("x" => 1))
            run!(warm, v, DataVault.keys(v)[3:4])
            opts = RunOpts(; status_interval=0.2, stuck_after=5.0)
            t = @async run!(work, v, ks; opts=opts)
            flagged = nothing
            t0 = time()
            while !istaskdone(t) && time() - t0 < 240
                st = read_status(v)
                if !isempty(st)
                    rows = [r for r in st[1]["worker_table"] if haskey(r, "stuck")]
                    if any(r -> r["key"] == ParamIO.canonical(ks[2]), rows)
                        flagged = (rows, st[1]["warnings"])
                        break
                    end
                end
                sleep(0.1)
            end
            touch(gate)
            r = fetch(t)
            @test r.done == 2                                 # said, not cut
            @test flagged !== nothing
            if flagged !== nothing
                @test only(flagged[1])["key"] == ParamIO.canonical(ks[2])
                @test any(w -> startswith(w, "stuck: 1 running key"), flagged[2])
            end
            ev = [e for e in _st_events(outdir) if e.kind == "key_stuck"]
            @test [e.key for e in ev] == [ParamIO.canonical(ks[2])]   # once, and not ks[1]
            @test only(ev).secs >= 5
            @test only(ev).stuck_after == 5.0
            # When it ended, nothing is stuck any more.
            @test !any(w -> startswith(w, "stuck"), only(read_status(v))["warnings"])
        end
    end
end

@testset "status: fewer workers joined than planned is said out loud, once" begin
    _st_workers(2) do
        _st_vault() do v, outdir
            note_workers!(; planned=5, launched=3)
            withenv("JULIA_WORKER_TIMEOUT" => "0") do
                run!(
                    k -> (sleep(0.3); Dict{String,Any}("x" => 1)),
                    v,
                    DataVault.keys(v);
                    opts=RunOpts(; status_interval=0.1),
                )
            end
            st = only(read_status(v))
            @test st["workers"]["planned"] == 5
            @test st["workers"]["launched"] == 3
            @test st["workers"]["joined"] == 2
            @test any(
                w -> startswith(w, "workers_short: planned 5, launched 3, joined 2"),
                st["warnings"],
            )
            short = [e for e in _st_events(outdir) if e.kind == "workers_short"]
            @test length(short) == 1
            @test (short[1].planned, short[1].launched, short[1].joined) == (5, 3, 2)
            @test occursin("! workers_short", sprint(io -> print_status(io, v)))
        end
    end
end

@testset "cli: a usage error is exit code 2 and prints the usage" begin
    io = IOBuffer()
    @test SweepRunner.cli(String[]; io=io) == 2
    @test SweepRunner.cli(["nope", "x"]; io=io) == 2
    @test SweepRunner.cli(["status"]; io=io) == 2
    @test occursin("usage: sweeprunner", String(take!(io)))
    @test SweepRunner.cli(["--help"]; io=IOBuffer()) == 0
end

# ── the alarm for a job that uses a fraction of its cores (#134) ─────────────────────────────────

@testset "warn-level events are also said where a person sees them, a bounded number of times (#134)" begin
    dir = mktempdir()
    log = SweepRunner.EventLog(joinpath(dir, "events_x.jsonl"))
    io = IOBuffer()
    SweepRunner.echo_warnings!(io)
    try
        before = SweepRunner.warning_counts()
        for i in 1:250
            SweepRunner.log_event(log, :test_warning_134; level=:warn, n=i, why="because")
        end
        SweepRunner.log_event(log, :test_info_134; n=1)                  # not a warning
        out = String(take!(io))
        lines = filter(l -> occursin("test_warning_134", l), split(out, '\n'))
        # One line for 250 events in the same minute: the first.
        @test length(lines) == 1
        @test occursin("why=because", lines[1]) && !occursin("×", lines[1])
        @test !occursin("test_info_134", out)
        # After the quiet minute (none, here) the kind is said again, with the count...
        SweepRunner._WARNING_EVERY[] = 0.0
        SweepRunner.log_event(log, :test_warning_134; level=:warn, n=251)
        SweepRunner._WARNING_EVERY[] = 60.0
        @test occursin("test_warning_134 (×251 so far)", String(take!(io)))
        # ...and an ERROR of a kind that was just said is said at once (#155): "the cut gave
        # up" must not be swallowed by "the cut began".
        SweepRunner.log_event(log, :test_warning_134; level=:error, n=252, gave_up=true)
        said = String(take!(io))
        @test occursin("sweeprunner error: test_warning_134", said)
        @test occursin("gave_up=true", said)
        SweepRunner.log_event(log, :test_warning_134; level=:warn, n=253)   # a warning: not yet
        @test isempty(String(take!(io)))
        @test SweepRunner._warnings_since(before)["test_warning_134"] == 253
        # All of them are in the file: the echo is not the log.
        @test count(l -> occursin("test_warning_134", l), readlines(log.path)) == 253
        SweepRunner.echo_warnings!(nothing)
        SweepRunner.log_event(log, :test_quiet_134; level=:warn)
        @test isempty(String(take!(io)))
    finally
        SweepRunner.echo_warnings!(:stderr)
    end
end

@testset "status: cores idle with keys queued is said — in the log, on stderr, at the top of the status (#134)" begin
    @test_throws ArgumentError RunOpts(; min_utilisation=1.5)
    io = IOBuffer()
    SweepRunner.echo_warnings!(io)
    SweepRunner._LOW_UTIL_AFTER[] = 0.5
    try
        _st_workers(2) do
            _st_vault() do v, outdir
                ks = DataVault.keys(v)                           # four keys for two workers
                gate = joinpath(outdir, "gate")
                work = k -> begin
                    t0 = time()
                    while !isfile(gate) && time() - t0 < 300
                        sleep(0.05)
                    end
                    return Dict{String,Any}("x" => 1)
                end
                # An allocation far larger than the two workers that came: the incident.
                withenv("SLURM_JOB_CPUS_PER_NODE" => "100000(x2)") do
                    t = @async run!(work, v, ks; opts=RunOpts(; status_interval=0.2))
                    seen = nothing
                    t0 = time()
                    while !istaskdone(t) && time() - t0 < 120
                        st = read_status(v)
                        if !isempty(st) &&
                            any(w -> startswith(w, "low_utilisation"), st[1]["warnings"])
                            seen = st[1]
                            break
                        end
                        sleep(0.1)
                    end
                    @test seen !== nothing
                    if seen !== nothing
                        @test seen["workers"]["cores_allocated"] == 200000
                        text = sprint(io -> print_status(io, v))
                        lines = split(text, '\n')
                        # The warning is the first thing under the master's line.
                        @test startswith(strip(lines[2]), "! low_utilisation")
                    end
                    touch(gate)
                    r = fetch(t)
                    @test r.done == length(ks)                   # said, nothing stopped
                end
                ev = [e for e in _st_events(outdir) if e.kind == "low_utilisation"]
                @test length(ev) == 1                            # once, not every tick
                @test ev[1].cores_allocated == 200000
                @test ev[1].queued >= 1
                @test occursin("allocated cores", ev[1].why)
                @test occursin("sweeprunner warning: low_utilisation", String(take!(io)))
            end
        end
        # An allocation that cannot be read is "not known", not 100% in use.
        _st_vault() do v, _
            withenv("SLURM_JOB_CPUS_PER_NODE" => "many") do
                run!(k -> Dict{String,Any}("x" => 1), v, DataVault.keys(v))
                st = only(read_status(v))
                @test st["workers"]["cores_allocated"] === nothing
                @test occursin("allocation not known", sprint(io -> print_status(io, v)))
            end
        end
    finally
        SweepRunner._LOW_UTIL_AFTER[] = 600.0
        SweepRunner.echo_warnings!(:stderr)
    end
end

# ── third review (#155) ──────────────────────────────────────────────────────────────────────────

@testset "a master is measured against the cores that are its own, not the whole job's (#155)" begin
    # A node group out of the job's allocation.
    env = (
        "SLURM_JOB_NODELIST" => "c[001-004]",
        "SLURM_JOB_CPUS_PER_NODE" => "128(x4)",
        "SWEEPRUNNER_NODELIST" => "c[001-002]",
    )
    withenv(env...) do
        @test SweepRunner._slurm_alloc_cores() == 512
        @test SweepRunner._slurm_group_cores() == 256
    end
    withenv("SWEEPRUNNER_NODELIST" => nothing) do
        @test SweepRunner._slurm_group_cores() == 0
    end
    _st_vault() do v, _
        m = withenv(
            "SLURM_JOB_ID" => "777",
            "SLURM_ARRAY_JOB_ID" => nothing,
            "SLURM_ARRAY_TASK_ID" => nothing,
        ) do
            return SweepRunner.Master()
        end
        @test m.job == "777"
        m.vault = v
        withenv("SLURM_JOB_CPUS_PER_NODE" => "128(x4)") do
            # Told its share: that.
            m.own_cores = 128
            @test SweepRunner._master_alloc_cores(m) == 128
            # Not told, and alone in the job: the job's.
            m.own_cores = 0
            @test SweepRunner._master_alloc_cores(m) == 512
            # Not told, with three other masters of the same job each holding a node: what is
            # left is its own. Each of four full masters is then at 100%, not at 25%.
            real = Dict{String,Any}(
                "stage" => "st",
                "state" => "running",
                "started" => time() - 10,
                "updated" => time() + 3600,
                "interval" => 60.0,
                "host" => "h",
                "pid" => 1,
                "tasks" => Dict{String,Any}(),
                "warnings" => String[],
                "nodes" => Any[],
                "nodes_without_workers" => String[],
                "worker_table" => Any[],
            )
            for (i, job) in enumerate(("777", "777", "777", "888"))
                id = "other_$i"
                dir = joinpath(state_root(v), "masters", id)
                mkpath(dir)
                st = merge(
                    real,
                    Dict{String,Any}(
                        "master" => id,
                        "job" => job,
                        "workers" => Dict{String,Any}("cores_joined" => 128),
                    ),
                )
                write(joinpath(dir, "status.json"), JSON3.write(st))
            end
            @test SweepRunner._master_alloc_cores(m) == 128      # 512 less 3 × 128; job 888 is not ours
        end
    end
end

@testset "a status check that throws is said, by name, and the others still run (#155)" begin
    _st_vault() do v, outdir
        ks = DataVault.keys(v)
        m = SweepRunner.Master()
        m.vault = v
        m.stage = "st"
        m.multi = true
        m.min_utilisation = 0.5
        m.stuck_after = 1.0
        log = SweepRunner.EventLog(joinpath(outdir, "events_x.jsonl"))
        SweepRunner._STATUS_EXTRA[] = m -> error("this check is broken")
        try
            for _ in 1:3
                SweepRunner.status_tick!(m, log)
            end
        finally
            SweepRunner._STATUS_EXTRA[] = nothing
        end
        ev = [e for e in _st_events(outdir) if e.kind == "status_tick_failed"]
        # Said once for three ticks, by name, with the count and what it threw.
        @test length(ev) == 1
        @test (ev[1].step, ev[1].count) == ("extra", 1)
        @test occursin("this check is broken", ev[1].err)
        # The step after them still ran: the status was written.
        @test isfile(SweepRunner.status_path(v, m))
    end
end
