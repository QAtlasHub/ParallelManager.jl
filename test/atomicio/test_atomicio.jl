using ParallelManager, Test

@testset "atomic_write: basic" begin
    mktempdir() do dir
        path = joinpath(dir, "sub", "data.txt")
        atomic_write(path) do io
            return write(io, "hello")
        end
        @test isfile(path)
        @test read(path, String) == "hello"
        # No .tmp leftovers
        @test all(!startswith(f, "data.txt.tmp") for f in readdir(joinpath(dir, "sub")))
    end
end

@testset "atomic_write: overwrite" begin
    mktempdir() do dir
        path = joinpath(dir, "data.txt")
        atomic_write(io -> write(io, "first"), path)
        atomic_write(io -> write(io, "second"), path)
        @test read(path, String) == "second"
    end
end

@testset "atomic_write: exception leaves target untouched" begin
    mktempdir() do dir
        path = joinpath(dir, "data.txt")
        atomic_write(io -> write(io, "original"), path)
        @test_throws ErrorException atomic_write(path) do io
            write(io, "partial")
            return error("boom")
        end
        @test read(path, String) == "original"
        # tmp cleaned up
        @test all(!startswith(f, "data.txt.tmp") for f in readdir(dir))
    end
end

@testset "atomic_write: exception when target did not exist" begin
    mktempdir() do dir
        path = joinpath(dir, "data.txt")
        @test_throws ErrorException atomic_write(path) do io
            write(io, "partial")
            return error("boom")
        end
        @test !isfile(path)
    end
end

@testset "atomic_touch" begin
    mktempdir() do dir
        path = joinpath(dir, "a", "b", ".done")
        atomic_touch(path)
        @test isfile(path)
        @test filesize(path) == 0
    end
end

@testset "atomic_write: concurrent writers produce some valid final content" begin
    mktempdir() do dir
        path = joinpath(dir, "data.txt")
        N = 20
        Threads.@threads for i in 1:N
            atomic_write(path) do io
                return write(io, string("writer-", i))
            end
        end
        @test isfile(path)
        final = read(path, String)
        @test startswith(final, "writer-")
        # Some writer won; the content is complete (not truncated)
        @test any(final == "writer-$i" for i in 1:N)
        # No stray tmp files
        @test all(!startswith(f, "data.txt.tmp") for f in readdir(dir))
    end
end
