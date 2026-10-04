using SweepRunner, Test, JSON3

@testset "EventLog: single write" begin
    mktempdir() do dir
        log = EventLog(joinpath(dir, "events.jsonl"))
        log_event(log, :stage_start; stage="phase1", todo=10)
        lines = readlines(log.path)
        @test length(lines) == 1
        rec = JSON3.read(lines[1])
        @test rec.kind == "stage_start"
        @test rec.stage == "phase1"
        @test rec.todo == 10
        @test haskey(rec, :ts)
    end
end

@testset "EventLog: all standard event kinds" begin
    mktempdir() do dir
        log = EventLog(joinpath(dir, "events.jsonl"))
        for k in (
            :stage_start,
            :stage_done,
            :key_start,
            :key_done,
            :lock_busy,
            :lock_reclaimed,
            :error,
            :gave_up,
            :retry,
            :skip_complete,
            :key_acquired,
            :lock_lost,
            :lock_reaped,
            :reap_failed,
            :worker_died,
            :worker_lost,
        )
            log_event(log, k; info="test")
        end
        lines = readlines(log.path)
        @test length(lines) == 16
        for line in lines
            rec = JSON3.read(line)
            @test haskey(rec, :kind)
            @test haskey(rec, :ts)
        end
    end
end

@testset "EventLog: concurrent writers do not tear lines" begin
    mktempdir() do dir
        log = EventLog(joinpath(dir, "events.jsonl"))
        N_tasks = 50
        N_events_per_task = 40
        tasks = Task[]
        for tid in 1:N_tasks
            let tid = tid
                push!(tasks, Threads.@spawn begin
                    for i in 1:N_events_per_task
                        log_event(log, :key_done; tid=tid, i=i)
                    end
                end)
            end
        end
        foreach(wait, tasks)

        lines = readlines(log.path)
        @test length(lines) == N_tasks * N_events_per_task
        # Every line must be valid JSON
        for line in lines
            rec = JSON3.read(line)
            @test haskey(rec, :tid)
            @test haskey(rec, :i)
        end
    end
end

@testset "EventLog: parent directory auto-created" begin
    mktempdir() do dir
        log = EventLog(joinpath(dir, "deep", "nested", "events.jsonl"))
        log_event(log, :stage_start)
        @test isfile(log.path)
    end
end

@testset "EventLog: no println API exported" begin
    # Structural check: log_event is the only per-event API
    names_exported = names(SweepRunner)
    @test :log_event in names_exported
    # No public function named `println_event`, `print_event`, etc.
    for n in names_exported
        s = String(n)
        @test !occursin("println", s)
    end
end

@testset "merge_event_logs: the masters' logs as one file, in time order" begin
    dir = mktempdir()
    a = EventLog(joinpath(dir, "events_hostA_1.jsonl"))
    b = EventLog(joinpath(dir, "events_hostB_2.jsonl"))
    log_event(a, :first; n=1)
    sleep(0.01)
    log_event(b, :second; n=2)
    sleep(0.01)
    log_event(a, :third; n=3)
    out = merge_event_logs(dir)
    @test out == joinpath(dir, "events_merged.jsonl")
    kinds = [JSON3.read(l).kind for l in readlines(out)]
    @test kinds == ["first", "second", "third"]
    # Merging again does not fold the merged file into itself.
    merge_event_logs(dir)
    @test length(readlines(out)) == 3
end
