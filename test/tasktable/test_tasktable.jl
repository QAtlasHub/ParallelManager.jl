# TaskTable (#64): the master's table of a round's units, and the queue drawn from it.

using SweepRunner, Test, ParamIO
using SweepRunner:
    Progress,
    next_task!,
    start_task!,
    settle!,
    hold!,
    requeue!,
    add_tasks!,
    settle_queued!,
    task_counts

function _tt_keys(n; group=i -> 1)
    return [ParamIO.DataKey(Dict{String,Any}("i" => i, "g" => group(i)), 1) for i in 1:n]
end
_tt_i(t, idx) = t.rows[idx].key.params["i"]

function _tt_drain(t, worker)
    out = Int[]
    while true
        i = next_task!(t, worker)
        i === nothing && break
        start_task!(t, i, "tok", worker)
        push!(out, _tt_i(t, i))
    end
    return out
end

@testset "TaskTable: without affinity the queue is the caller's order" begin
    t = TaskTable(_tt_keys(6))
    @test length(t) == 6
    @test task_counts(t).todo == 6
    @test _tt_drain(t, 2) == 1:6
    @test task_counts(t).running == 6
    @test next_task!(t, 2) === nothing
end

@testset "TaskTable: a worker stays on its group, and a free one takes the largest" begin
    # group 1: i = 1..2, group 2: i = 3..8
    ks = _tt_keys(8; group=i -> i <= 2 ? 1 : 2)
    t = TaskTable(ks; affinity=k -> k.params["g"])
    first_a = next_task!(t, 10)
    @test t.rows[first_a].group == 2          # the largest group, not the first key
    @test _tt_i(t, first_a) == 3              # and its first key
    # A second worker goes to the group with the most left, which is still group 2 (5 > 2).
    first_b = next_task!(t, 11)
    @test t.rows[first_b].group == 2
    # Worker 10 keeps drawing group 2, in order, until it is empty; only then group 1.
    rest = Int[]
    while (i = next_task!(t, 10)) !== nothing
        push!(rest, _tt_i(t, i))
    end
    @test rest == [5, 6, 7, 8, 1, 2]
end

@testset "TaskTable: rows that left the queue are not handed out" begin
    t = TaskTable(_tt_keys(4))
    settle!(t, 1, :already_done)
    hold!(t, 2, "other:1:abcd")
    @test t.rows[2].state === :held
    @test t.rows[2].outcome === :lock_busy
    @test _tt_drain(t, 2) == [3, 4]
    c = task_counts(t)
    @test (c.done, c.held, c.running, c.todo) == (1, 1, 2, 0)
end

@testset "TaskTable: requeue goes behind, front=true goes ahead of everything" begin
    t = TaskTable(_tt_keys(4))
    i = next_task!(t, 2)
    start_task!(t, i, "tok", 2)
    requeue!(t, i)                     # behind 2, 3, 4
    @test _tt_drain(t, 2) == [2, 3, 4, 1]

    t = TaskTable(_tt_keys(4))
    hold!(t, 3, nothing)
    requeue!(t, 3; front=true)         # ahead of 1, 2, 4
    @test _tt_drain(t, 2) == [3, 1, 2, 4]
    @test t.rows[3].owner == "tok"
end

@testset "TaskTable: add_tasks! queues behind what is there and ignores known keys" begin
    ks = _tt_keys(5)
    t = TaskTable(ks[1:3])
    @test add_tasks!(t, ks[2:5]) == 2          # 2 and 3 are already rows
    @test length(t) == 5
    @test _tt_drain(t, 2) == 1:5
end

@testset "TaskTable: a stop settles every queued row and names why" begin
    t = TaskTable(_tt_keys(5))
    i = next_task!(t, 2)
    start_task!(t, i, "tok", 2)
    @test settle_queued!(t, :stop_flag) == 4
    @test next_task!(t, 2) === nothing
    @test count(r -> r.outcome === :stop_flag, t.rows) == 4
    @test t.rows[i].state === :running          # the one that is out is not touched
    settle!(t, i, :ok)
    c = task_counts(t)
    @test (c.done, c.other, c.todo, c.running) == (1, 4, 0, 0)
end

@testset "TaskTable: settle!(:ok) forgets the resume point, an error keeps it" begin
    t = TaskTable(_tt_keys(2))
    t.rows[1].progress = Progress(3, 8, time(), "")
    t.rows[2].progress = Progress(3, 8, time(), "")
    settle!(t, 1, :ok)
    settle!(t, 2, :error)
    @test t.rows[1].progress === nothing
    @test t.rows[2].progress.step == 3
    @test task_counts(t).failed == 1
end

@testset "TaskTable: an outcome is one of a closed set (#110)" begin
    t = TaskTable(_tt_keys(3))
    @test_throws ArgumentError settle!(t, 1, :typo)
    @test_throws ArgumentError settle_queued!(t, :typo)
    @test t.rows[1].state === :todo                             # nothing was changed
    @test task_counts(t).todo == 3
    for o in SweepRunner.OUTCOMES
        settle!(t, 1, o)
        @test t.rows[1].outcome === o
    end
    # Held by a sibling is counted as held, whether the scan saw it or a worker ran into it.
    hold!(t, 2, "other:1:abcd")
    settle!(t, 3, :lock_busy)
    settle!(t, 1, :ok)
    c = task_counts(t)
    @test (c.done, c.held, c.other) == (1, 2, 0)
end
