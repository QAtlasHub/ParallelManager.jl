"""
LogisticMap — a dependency-free toy "physics" kernel for the examples.

The logistic map  x_{n+1} = r · x_n · (1 − x_n)  is the textbook route to
chaos. As the control parameter `r` grows it goes through period-doubling
into a chaotic regime near r ≈ 3.5699, with a fully chaotic band at r = 4.

We use it because it is:
- pure Julia, zero dependencies (so the example runs anywhere `julia` runs),
- a real "sweep one control parameter, extract one observable" workload —
  structurally identical to a DMRG / Monte-Carlo sweep, just 1000× faster.

`lyapunov(r, x0)` is the observable the example computes per parameter point.
It is **independent of the initial condition** `x0` (a property of the
attractor), which is why different `sample` values agree — a convenient way
to show reproducibility.
"""
module LogisticMap

export logistic_step, lyapunov, orbit_tail

logistic_step(r::Float64, x::Float64) = r * x * (1 - x)

"""
    lyapunov(r, x0; transient=1000, nsteps=20000) -> Float64

Largest Lyapunov exponent λ(r) = ⟨ln|f′(x)|⟩ along the orbit, with
f′(x) = r(1 − 2x). λ > 0 ⇒ chaos; λ < 0 ⇒ periodic/fixed point.
"""
function lyapunov(r::Float64, x0::Float64; transient::Int=1000, nsteps::Int=20000)::Float64
    x = x0
    for _ in 1:transient
        x = logistic_step(r, x)
    end
    s = 0.0
    for _ in 1:nsteps
        s += log(abs(r * (1 - 2x)) + eps())   # +eps guards the x = 0.5 critical point
        x = logistic_step(r, x)
    end
    return s / nsteps
end

"""
    orbit_tail(r, x0; transient=1000, ntail=64) -> Vector{Float64}

The last `ntail` iterates after discarding the transient — the attractor
itself (a fixed point, a 2/4/8-cycle, or a chaotic band).
"""
function orbit_tail(r::Float64, x0::Float64; transient::Int=1000, ntail::Int=64)::Vector{Float64}
    x = x0
    for _ in 1:transient
        x = logistic_step(r, x)
    end
    tail = Vector{Float64}(undef, ntail)
    for i in 1:ntail
        x = logistic_step(r, x)
        tail[i] = x
    end
    return tail
end

end # module LogisticMap
