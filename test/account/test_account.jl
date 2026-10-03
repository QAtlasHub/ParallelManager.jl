# Account (#80): where a job's core-hours went — computing (kept / lost), start-up, never started,
# idle by reason.

using SweepRunner, Test, DataVault, ParamIO, JSON3, Distributed
using SweepRunner: Master, Account

const _AC_CFG = joinpath(@__DIR__, "..", "run", "fixtures", "study.toml")

function _ac_vault(f; run="ac")
    outdir = mktempdir()
    try
        f(DataVault.Vault(_AC_CFG; run=run, outdir=outdir), outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

function _ac_events(outdir)
    logs = filter(f -> startswith(f, "events_") && endswith(f, ".jsonl"), readdir(outdir))
    return [JSON3.read(l) for f in logs for l in readlines(joinpath(outdir, f))]
end

@testset "Account: a key's time is kept up to its last progress and lost after it" begin
    a = Account()
    SweepRunner._acct_join!(a, 2, 4)
    SweepRunner._acct_join!(a, 2, 8)                      # joining twice changes nothing
    @test a.cores[2] == 4

    SweepRunner._acct_key!(a, 2, 100.0, 110.0, true, nothing)        # finished: all kept
    @test (a.busy, a.kept, a.lost, a.keys_cut) == (40.0, 40.0, 0.0, 0)
    SweepRunner._acct_key!(a, 2, 110.0, 120.0, false, 116.0)         # cut 4 s after progress
    @test (a.busy, a.kept, a.lost, a.keys_cut) == (80.0, 64.0, 16.0, 1)
    SweepRunner._acct_key!(a, 2, 120.0, 125.0, false, nothing)       # never reported: all lost
    @test (a.busy, a.kept, a.lost, a.keys_cut) == (100.0, 64.0, 36.0, 2)
    # Came straight back (someone else held it): the time is lost, but no work was cut.
    SweepRunner._acct_key!(a, 2, 125.0, 126.0, false, nothing; cut=false)
    @test (a.lost, a.keys_cut) == (40.0, 2)
    @test a.first_key[2] == 100.0                          # the first key, not the last

    SweepRunner._acct_idle!(a, 2, :queue_empty, 0.0, 10.0)
    SweepRunner._acct_idle!(a, 2, :queue_empty, 20.0, 25.0)
    SweepRunner._acct_idle!(a, 2, :lock_busy, 0.0, 1.0)
    @test a.idle == Dict(:queue_empty => 60.0, :lock_busy => 4.0)
end

@testset "account_snapshot: start-up, never started, and what is left" begin
    m = Master()
    t = m.started
    SweepRunner._acct_join!(m.acct, 2, 2; at=t + 5)
    SweepRunner._acct_join!(m.acct, 3, 2; at=t + 5)
    SweepRunner._acct_key!(m.acct, 2, t + 10, t + 60, true, nothing)   # 100 core-s, first key at 10
    SweepRunner._acct_idle!(m.acct, 2, :queue_empty, t + 60, t + 100)  # 80 core-s
    try
        note_workers!(; planned=5)                          # two joined, three never did
        s = withenv("SLURM_JOB_CPUS_PER_NODE" => "8(x2)") do
            account_snapshot(m; now=t + 100)
        end
        @test s["elapsed"] == 100.0
        @test s["allocated"] == 16 * 100.0
        @test (s["computing"], s["kept"], s["lost"], s["keys_cut"]) ==
            (100.0, 100.0, 0.0, 0)
        # Worker 2 waited 10 s for its first key; worker 3 never had one: the whole 100 s.
        @test s["startup"] == 2 * 10 + 2 * 100
        @test s["never_started"] == 3 * 2.0 * 100
        @test s["idle"] == Dict("queue_empty" => 80.0)
        @test s["other"] == 1600 - 100 - 220 - 600 - 80

        text = sprint(io -> print_account(io, s))
        @test occursin("allocated", text) && occursin("never started", text)
        @test occursin("queue_empty", text) && occursin("0 key(s) cut", text)
        # Without an allocation to read, the cores that joined are what was allocated.
        s2 = withenv("SLURM_JOB_CPUS_PER_NODE" => nothing) do
            account_snapshot(m; now=t + 100)
        end
        @test s2["allocated"] == 4 * 100.0
    finally
        note_workers!(; planned=0, launched=0)
    end
end

@testset "run!: the account is in the status and in the event log when the master ends" begin
    _ac_vault() do v, outdir
        ks = DataVault.keys(v)
        bad = ParamIO.canonical(ks[1])
        work = k -> begin
            if ParamIO.canonical(k) == bad
                sleep(0.3)
                SweepRunner.report_progress(1; of=2)
                sleep(0.4)
                error("cut after the first half")
            end
            sleep(0.2)
            return Dict{String,Any}("x" => 1)
        end
        r = run!(work, v, ks; opts=RunOpts(; max_attempts=1))
        @test (r.done, r.err) == (length(ks) - 1, 1)

        st = only(read_status(v))
        a = st["account"]
        cores = only(st["worker_table"])["cores"]
        @test a["keys_cut"] == 1
        @test a["computing"] ≈ a["kept"] + a["lost"]
        # The failed key kept its first 0.3 s and lost the 0.4 s after its progress stamp.
        @test 0.35 * cores <= a["lost"] <= 0.9 * cores
        @test a["kept"] >= (0.3 + 0.2 * (length(ks) - 1)) * cores * 0.9
        @test a["computing"] <= a["allocated"] + 1e-6
        @test a["startup"] >= 0

        ev = only([e for e in _ac_events(outdir) if e.kind == "job_account"])
        @test ev.account.keys_cut == 1
        @test ev.account.lost ≈ a["lost"]

        text = sprint(io -> print_account(io, v))
        @test occursin("1 key(s) cut", text) && occursin("computing", text)
        @test SweepRunner.cli(["account", outdir]; io=IOBuffer()) == 0
        @test sprint(io -> print_account(io, mktempdir())) == "no account found\n"
    end
end

@testset "run_loop!: one account for all its rounds" begin
    _ac_vault() do v, outdir
        ks = DataVault.keys(v)
        r = run_loop!(k -> (sleep(0.1); Dict{String,Any}("x" => 1)), v, ks)
        @test r.done == length(ks)
        ev = only([e for e in _ac_events(outdir) if e.kind == "job_account"])
        @test ev.account.keys_cut == 0
        @test ev.account.lost == 0
        @test ev.account.kept > 0
    end
end

@testset "workers: an idle worker's time is booked under why it was idle" begin
    nprocs() > 1 && rmprocs(workers())
    addprocs(2; exeflags="--project=$(dirname(Base.active_project()))")
    try
        @everywhere workers() Core.eval(
            Main, :(using SweepRunner, DataVault, ParamIO, Distributed)
        )
        # One key for two workers: the other waits on an empty queue.
        _ac_vault() do v, _
            ks = DataVault.keys(v)
            run!(k -> (sleep(1.5); Dict{String,Any}("x" => 1)), v, ks[1:1])
            a = only(read_status(v))["account"]
            @test get(a["idle"], "queue_empty", 0.0) >= 1.0
            @test !haskey(a["idle"], "lock_busy")
        end
        # Two keys, one held by a live sibling: the worker without a key waits on that lock.
        _ac_vault() do v, _
            ks = DataVault.keys(v)
            sib = owner_token()
            @test DataVault.acquire_running!(v, ks[2], sib) === :ok
            r = run!(k -> (sleep(1.5); Dict{String,Any}("x" => 1)), v, ks[1:2])
            @test (r.done, r.busy) == (1, 1)
            a = only(read_status(v))["account"]
            @test get(a["idle"], "lock_busy", 0.0) >= 1.0
            DataVault.clear_running!(v, ks[2], sib)
        end
    finally
        rmprocs(workers())
        note_workers!(; planned=0, launched=0)
    end
end
