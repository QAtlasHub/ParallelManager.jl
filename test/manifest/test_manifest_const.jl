using ParallelManager, Test, ParamIO

# A ParamIO `{const = X}` param yields a DataKey whose value is a *vector* (one fixed value), not a
# swept scalar. The Manifest keys on `ParamIO.canonical`, so it must (a) distinguish such keys by
# their whole vector value, and (b) round-trip the resulting canonical string — which contains
# brackets/spaces, e.g. `system.J=[1.0, 0.5, 0.5]` — through its JLD2 store. Grid axes need no
# separate coverage here: a grid expands to ordinary scalar swept keys (test_manifest.jl already
# exercises those); const is the only form that introduces a vector-valued key.

isdefined(@__MODULE__, :mkkey) ||
    (mkkey(params; sample=0) = DataKey(Dict{String,Any}(params...), sample))

@testset "Manifest: const-vector key round-trips through JLD2" begin
    mktempdir() do dir
        m = load_manifest(dir, :phase1)
        k = mkkey(["system.N" => 32, "system.J" => [1.0, 0.5, 0.5]]; sample=1)
        add_complete!(m, k)
        save_manifest(m)

        m2 = load_manifest(dir, :phase1)
        @test is_complete(m2, k)            # the bracketed [1.0, 0.5, 0.5] canonical survived save/load
        @test length(m2.complete) == 1
    end
end

@testset "Manifest: distinct const-vectors are distinguished" begin
    mktempdir() do dir
        m = load_manifest(dir, :phase1)
        kJ1 = mkkey(["system.J" => [1.0, 0.5, 0.5]]; sample=1)
        kJ2 = mkkey(["system.J" => [2.0, 1.0, 1.0]]; sample=1)   # a different fixed vector
        @test canonical(kJ1) != canonical(kJ2)

        add_complete!(m, kJ1)
        @test is_complete(m, kJ1)
        @test !is_complete(m, kJ2)          # completing one vector must NOT mark the other done
        @test todo_keys(m, [kJ1, kJ2]) == [kJ2]
    end
end

@testset "Manifest: a const-vector is distinct from the matching scalar sweep" begin
    mktempdir() do dir
        m = load_manifest(dir, :phase1)
        kvec = mkkey(["system.J" => [1.0, 0.5]]; sample=1)   # one fixed 2-vector (const)
        ks1 = mkkey(["system.J" => 1.0]; sample=1)           # a swept scalar point
        ks2 = mkkey(["system.J" => 0.5]; sample=1)
        @test canonical(kvec) != canonical(ks1)
        @test canonical(kvec) != canonical(ks2)

        add_complete!(m, kvec)
        @test is_complete(m, kvec)
        @test !is_complete(m, ks1)          # the vector value ≠ either scalar point
        @test !is_complete(m, ks2)
        @test todo_keys(m, [kvec, ks1, ks2]) == [ks1, ks2]
    end
end
