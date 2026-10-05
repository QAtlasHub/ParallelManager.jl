using SweepRunner
using Documenter
using Downloads

assets_dir = joinpath(@__DIR__, "src", "assets")
mkpath(assets_dir)
favicon_path = joinpath(assets_dir, "favicon.ico")
logo_path = joinpath(assets_dir, "logo.png")

Downloads.download("https://github.com/sotashimozono.png", favicon_path)
Downloads.download("https://github.com/sotashimozono.png", logo_path)

makedocs(;
    sitename="SweepRunner.jl",
    format=Documenter.HTML(;
        canonical="https://qatlashub.github.io/SweepRunner.jl/stable/",
        prettyurls=get(ENV, "CI", "false") == "true",
        mathengine=MathJax3(
            Dict(
                :tex => Dict(
                    :inlineMath => [["\$", "\$"], ["\\(", "\\)"]],
                    :tags => "ams",
                    :packages => ["base", "ams", "autoload", "physics"],
                ),
            ),
        ),
        assets=["assets/favicon.ico", "assets/custom.css"],
    ),
    modules=[SweepRunner],
    checkdocs=:exports,
    pages=[
        "Home" => "index.md",
        "Quick start" => "quickstart.md",
        "Architecture" => "architecture.md",
        "API reference" => "api.md",
        "API: control plane" => "api_plane.md",
        "API: pool, campaigns, jobs" => "api_campaign.md",
        "Guides" => "guides.md",
    ],
)

deploydocs(;
    repo="github.com/QAtlasHub/SweepRunner.jl.git", devbranch="main", push_preview=true
)
