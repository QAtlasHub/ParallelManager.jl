# The documentation's examples are held to the code (#109): the campaign the guide shows loads
# and is launchable, a name the pages call exists and is reachable the way it is written, and the
# event log is called what `run!` calls it.

using SweepRunner, Test, DataVault

const _DOC_ROOT = normpath(joinpath(@__DIR__, "..", ".."))
const _DOC_PAGES = vcat(
    [
        joinpath(_DOC_ROOT, "docs", "src", f) for
        f in readdir(joinpath(_DOC_ROOT, "docs", "src")) if endswith(f, ".md")
    ],
    [joinpath(_DOC_ROOT, "README.md"), joinpath(_DOC_ROOT, "CLAUDE.md")],
)

# The fenced blocks of one language in a page.
function _doc_blocks(text, lang)
    return [m.captures[1] for m in eachmatch(Regex("```$lang\\n(.*?)```", "s"), text)]
end

_doc_config(project) = """
[study]
project_name  = "$project"
total_samples = 1
outdir        = "out"

[datavault]
path_keys = ["N"]

[[paramsets]]
N = [4, 8]
"""

@testset "the guide's campaign file loads and is launchable" begin
    guide = read(joinpath(_DOC_ROOT, "docs", "src", "guides.md"), String)
    metas = filter(b -> occursin("[campaign]", b), _doc_blocks(guide, "toml"))
    @test !isempty(metas)
    for meta in metas
        dir = mktempdir()
        try
            # Every config the example names, one project per study as its file names say, so
            # that only what the example SAYS is tested.
            for m in eachmatch(r"\"([A-Za-z0-9_]+\.toml)\"", meta)
                file = m.captures[1]
                write(joinpath(dir, file), _doc_config(first(split(file, "_"))))
            end
            path = joinpath(dir, "campaign.toml")
            write(path, meta)
            c = load_campaign(path)
            report = validate_campaign(c)
            @test launchable(report)
            @test n_errors(report) == 0
        finally
            rm(dir; recursive=true, force=true)
        end
    end
end

@testset "a name the pages refer to exists" begin
    for page in _DOC_PAGES
        text = read(page, String)
        for m in eachmatch(r"\(@ref SweepRunner\.([A-Za-z_][A-Za-z0-9_]*!?)\)", text)
            name = Symbol(m.captures[1])
            isdefined(SweepRunner, name) || @error "no such name" page name
            @test isdefined(SweepRunner, name)
        end
        for m in eachmatch(r"\bSweepRunner\.([A-Za-z_][A-Za-z0-9_]*!?)\(", text)
            name = Symbol(m.captures[1])
            isdefined(SweepRunner, name) || @error "no such name" page name
            @test isdefined(SweepRunner, name)
        end
    end
end

@testset "a call written without the module is one `using SweepRunner` provides" begin
    exported = Set(names(SweepRunner))
    for page in _DOC_PAGES
        text = read(page, String)
        # `name(` at the start of an inline code span: written as something to call as is.
        for m in eachmatch(r"(?<!`)`([a-z][A-Za-z0-9_]*!?)\(", text)
            name = Symbol(m.captures[1])
            isdefined(SweepRunner, name) || continue          # someone else's function
            parentmodule(getfield(SweepRunner, name)) === SweepRunner || continue
            name in exported || @error "not exported, and written unqualified" page name
            @test name in exported
        end
    end
end

@testset "the event log is called what run! calls it" begin
    for page in _DOC_PAGES
        @test !occursin(r"(?<![_a-z])events\.jsonl", read(page, String))
    end
    outdir = mktempdir()
    try
        cfg = joinpath(outdir, "study.toml")
        write(cfg, _doc_config("doc"))
        v = DataVault.Vault(cfg; run="doc", outdir=outdir)
        run!(k -> Dict{String,Any}("x" => 1), v, DataVault.keys(v))
        logs = filter(f -> endswith(f, ".jsonl"), readdir(outdir))
        @test !isempty(logs)
        @test all(f -> occursin(r"^events_.+_\d+\.jsonl$", f), logs)
    finally
        rm(outdir; recursive=true, force=true)
    end
end
