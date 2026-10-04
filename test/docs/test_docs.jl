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

# ── what the first version of this file could not see (#141) ─────────────────────────────────────

const _DOC_SRC = joinpath(_DOC_ROOT, "src")
function _doc_src_text()
    return join((read(joinpath(_DOC_SRC, f), String) for f in readdir(_DOC_SRC)), "\n")
end

@testset "the guide's [jobs] block loads as a policy" begin
    guide = read(joinpath(_DOC_ROOT, "docs", "src", "guides.md"), String)
    blocks = filter(b -> occursin("[jobs]", b), _doc_blocks(guide, "toml"))
    @test !isempty(blocks)
    for b in blocks
        dir = mktempdir()
        try
            path = joinpath(dir, "campaign.toml")
            write(path, b)
            policy = load_job_policy(path)
            @test !isempty(policy.partitions)
            @test policy.dry_run                                  # the example does not submit
        finally
            rm(dir; recursive=true, force=true)
        end
    end
end

@testset "an event a recipe selects is one the default log level writes" begin
    src = _doc_src_text()
    for page in _DOC_PAGES
        for block in vcat(
            _doc_blocks(read(page, String), "bash"), _doc_blocks(read(page, String), "sh")
        )
            for m in eachmatch(r"\.kind == \"([a-z_]+)\"", block)
                kind = m.captures[1]
                # Each place the kind is logged, with the level it is logged at: `level=`
                # wherever it stands among the call's keywords (none: info).
                levels = String[]
                for r in findall(":$kind;", src)
                    call = first(
                        split(src[last(r):min(last(r) + 400, lastindex(src))], ")\n")
                    )
                    m2 = match(r"level=:([a-z]+)", call)
                    push!(levels, m2 === nothing ? "info" : m2.captures[1])
                end
                isempty(levels) && @error "no such event" page kind
                @test !isempty(levels)
                written = any(!=("debug"), levels)
                # Emitted only at debug: the recipe has to say that it needs the debug level.
                written || @test occursin("log_level", block)
            end
        end
    end
end

@testset "the module tables list every file of src/" begin
    files = filter(f -> endswith(f, ".jl") && f != "SweepRunner.jl", readdir(_DOC_SRC))
    for page in (
        joinpath(_DOC_ROOT, "docs", "src", "architecture.md"),
        joinpath(_DOC_ROOT, "README.md"),
    )
        text = read(page, String)
        for f in files
            occursin("src/$f", text) || @error "not in the module table" page f
            @test occursin("src/$f", text)
        end
    end
    # The table in the package's own docstring, and the list in CLAUDE.md (by module name).
    mod = read(joinpath(_DOC_SRC, "SweepRunner.jl"), String)
    claude = read(joinpath(_DOC_ROOT, "CLAUDE.md"), String)
    for f in files
        occursin("`$f`", mod) || @error "not in the module docstring" f
        @test occursin("`$f`", mod)
        name = replace(f, ".jl" => "")
        occursin("`$name`", claude) || @error "not in CLAUDE.md's module layout" name
        @test occursin("`$name`", claude)
    end
end

@testset "the exit codes the CLI documents are the ones it returns" begin
    cli = read(joinpath(_DOC_SRC, "CLI.jl"), String)
    doc = match(r"The `sweeprunner` command line\. Returns the exit code:(.*?)\n```"s, cli)
    @test doc !== nothing
    documented = Set(
        parse(Int, m.captures[1]) for m in eachmatch(r"\| `(\d)` \|", doc.captures[1])
    )
    returned = Set(parse(Int, m.captures[1]) for m in eachmatch(r"\breturn (\d)\b", cli))
    union!(returned, parse(Int, m.captures[1]) for m in eachmatch(r"code = (\d)\b", cli))
    @test returned ⊆ documented
    @test documented ⊆ union(returned, Set([0]))
    # The usage text a person reads names them too.
    usage = match(r"const _CLI_USAGE = \"\"\"(.*?)\"\"\""s, cli)
    @test usage !== nothing
    if usage !== nothing
        for code in setdiff(documented, Set([0]))
            occursin(Regex("\\b$code\\b"), usage.captures[1]) ||
                @error "exit code not in the usage text" code
            @test occursin(Regex("\\b$code\\b"), usage.captures[1])
        end
    end
end
