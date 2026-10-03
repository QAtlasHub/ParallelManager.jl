# CLI — asking a sweep from a shell.
#
#     sweeprunner status <outdir> [--workers] [--json]
#     sweeprunner locks  <outdir>
#
# `bin/sweeprunner` is the wrapper; `julia -e 'using SweepRunner; SweepRunner.cli(ARGS)' -- …` is
# the same thing. Everything here reads (or, for requests, writes one small file under) the
# sweep's state directory, so it runs on a login node while the job runs on the compute nodes.

const _CLI_USAGE = """
usage: sweeprunner <command> <outdir> [options]

  status <outdir> [--workers] [--json]
      What every master under <outdir> is doing: task counts, workers planned / launched /
      joined / busy, cores in use, nodes with no worker, warnings. --workers adds one line per
      worker; --json prints the status files as one JSON array.

  locks <outdir>
      Every .running lock under <outdir>: who holds it, heartbeat and progress age, and whether
      its holder's master says it is held, dead, or cannot be asked. Removes nothing.
"""

"""
    cli(args=ARGS; io=stdout) -> Int

The `sweeprunner` command line. Returns the exit code: `0`, or `2` for a usage error (the usage
text is printed to `io`).

```
sweeprunner status out/campaign --workers
```
"""
function cli(args::AbstractVector{<:AbstractString}=ARGS; io::IO=stdout)
    if isempty(args) || args[1] in ("-h", "--help", "help")
        print(io, _CLI_USAGE)
        return isempty(args) ? 2 : 0
    end
    cmd = args[1]
    rest = args[2:end]
    flags = Set(a for a in rest if startswith(a, "--"))
    pos = [a for a in rest if !startswith(a, "--")]
    if cmd == "status"
        length(pos) == 1 || return _cli_usage(io, "status takes one <outdir>")
        if "--json" in flags
            JSON3.write(io, read_status(pos[1]))
            println(io)
        else
            print_status(io, pos[1]; workers="--workers" in flags)
        end
        return 0
    end
    if cmd == "locks"
        length(pos) == 1 || return _cli_usage(io, "locks takes one <outdir>")
        print_locks(io, pos[1])
        return 0
    end
    return _cli_usage(io, "unknown command: $cmd")
end

function _cli_usage(io::IO, msg::AbstractString)
    println(io, "sweeprunner: ", msg)
    print(io, _CLI_USAGE)
    return 2
end

export cli
