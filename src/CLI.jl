# CLI — asking a sweep from a shell.
#
#     sweeprunner status <outdir> [--workers] [--json]
#     sweeprunner locks  <outdir>
#     sweeprunner pause|resume|stop|cancel|prioritise|resize|drain|enqueue <outdir> [options]
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

  pause | resume <outdir>
  stop <outdir> [--select name=v1,v2 ...] [--node HOST] [--grace SECONDS] [--interrupt]
  cancel <outdir> --select name=v1,v2 [...] [--samples 1,2] [--running] [--grace SECONDS]
  prioritise <outdir> --select name=v1,v2 [...] [--samples 1,2]
  resize <outdir> --n N
  drain <outdir> --node HOST
  enqueue <outdir> --config FILE
      Requests to the masters running under <outdir>; see `SweepRunner.control!`. Limit them with
      --project NAME, --run NAME, --master ID. Prints the request ids.
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
    Symbol(cmd) in _CONTROL_OPS && return _cli_control(io, Symbol(cmd), rest)
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

# Options that take a value, and the ones that do not.
const _CLI_VALUED = (
    "--select",
    "--samples",
    "--node",
    "--grace",
    "--n",
    "--config",
    "--project",
    "--run",
    "--master",
)
const _CLI_SWITCHES = ("--running", "--interrupt")

# `32` -> 32, `0.5` -> 0.5, `true` -> true, anything else stays a string.
function _cli_value(s::AbstractString)
    for T in (Int, Float64, Bool)
        v = tryparse(T, s)
        v === nothing || return v
    end
    return String(s)
end

function _cli_control(io::IO, op::Symbol, rest)
    outdir = nothing
    select = Dict{String,Vector{Any}}()
    kw = Dict{Symbol,Any}()
    i = 1
    while i <= length(rest)
        a = rest[i]
        if a in _CLI_SWITCHES
            kw[Symbol(a[3:end])] = true
        elseif a in _CLI_VALUED
            i < length(rest) || return _cli_usage(io, "$a needs a value")
            v = rest[i += 1]
            if a == "--select"
                nv = split(v, '='; limit=2)
                length(nv) == 2 || return _cli_usage(io, "--select takes name=v1,v2")
                select[String(nv[1])] = Any[_cli_value(x) for x in split(nv[2], ',')]
            elseif a == "--samples"
                kw[:samples] = [parse(Int, x) for x in split(v, ',')]
            elseif a == "--grace"
                kw[:grace] = parse(Float64, v)
            elseif a == "--n"
                kw[:n] = parse(Int, v)
            else
                kw[Symbol(a[3:end])] = String(v)
            end
        elseif startswith(a, "--")
            return _cli_usage(io, "unknown option: $a")
        elseif outdir === nothing
            outdir = a
        else
            return _cli_usage(io, "$op takes one <outdir>")
        end
        i += 1
    end
    outdir === nothing && return _cli_usage(io, "$op needs an <outdir>")
    isempty(select) || (kw[:select] = select)
    ids = try
        control!(outdir, op; kw...)
    catch e
        e isa ArgumentError || rethrow()
        return _cli_usage(io, e.msg)
    end
    if isempty(ids)
        println(io, "no sweep state under $outdir: nothing to send the request to")
        return 1
    end
    foreach(id -> println(io, id), ids)
    return 0
end

function _cli_usage(io::IO, msg::AbstractString)
    println(io, "sweeprunner: ", msg)
    print(io, _CLI_USAGE)
    return 2
end

export cli
