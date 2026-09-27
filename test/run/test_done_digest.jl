using SweepRunner, Test, DataVault, ParamIO

# The `.done` a sweep writes has to name the bytes the sweep produced. Without it a later reader
# — `Pinax.report` hashing what it loads, `Archeion.provenance_from` comparing the two — can only
# report `result_unknown`, and the comparison that would catch a data file edited or truncated
# between computing and reporting never happens. `save!` returns the digest and `mark_done!`
# accepts it; the gap was the call between them.
#
# This is checked from the consumer's side, through `load_recorded`, because that is who suffers:
# nothing inside the runner notices when the digest is missing — the sweep succeeds, every key is
# `:done`, and only a reader two packages away can tell.
const FIXTURE_CFG_DD = joinpath(@__DIR__, "fixtures", "study.toml")

function with_vault_dd(f; run::AbstractString="digest")
    outdir = mktempdir()
    try
        f(DataVault.Vault(FIXTURE_CFG_DD; run=run, outdir=outdir), outdir)
    finally
        rm(outdir; recursive=true, force=true)
    end
end

@testset "a completed key names the bytes it produced" begin
    with_vault_dd() do v, outdir
        keys = ParamIO.expand(v.spec)
        run!(
            k -> Dict{String,Any}("x" => 1.0), v, keys; opts=RunOpts(; workers=:sequential)
        )
        @test !isempty(keys)
        for k in keys
            @test DataVault.is_done(v, k)
            _, rec = DataVault.load_recorded(v, k)

            # `"unknown"` is what these say when `mark_done!` is called without `result=`, which
            # is how every sweep this runner drove was written before.
            @test rec.done_version == "2"
            @test rec.result_sha256 != "unknown"

            # Present is not enough: the digest has to be OF THE FILE. A plausible-looking hash of
            # the wrong bytes passes every check above and fails this one.
            @test rec.read_sha256 == rec.result_sha256
            @test isfile(joinpath(outdir, rec.file))
        end
    end
end

@testset "a result replaced behind the runner's back is caught" begin
    with_vault_dd(; run="digest_tamper") do v, outdir
        keys = ParamIO.expand(v.spec)
        run!(
            k -> Dict{String,Any}("x" => 3.0), v, keys; opts=RunOpts(; workers=:sequential)
        )
        k = first(keys)
        _, before = DataVault.load_recorded(v, k)

        # Rewrite the result with different numbers, leaving the `.done` marker alone — the shape
        # of an edited or half-restored vault. Nothing about the key's status changes.
        DataVault.save!(v, k, Dict{String,Any}("x" => 99.0))
        @test DataVault.is_done(v, k)

        data, after = DataVault.load_recorded(v, k)
        @test data["x"] == 99.0
        @test after.result_sha256 == before.result_sha256      # what the computation recorded
        @test after.read_sha256 != after.result_sha256         # what is there now: they differ
    end
end
