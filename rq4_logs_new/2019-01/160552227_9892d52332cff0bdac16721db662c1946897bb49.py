import julia

j = julia.Julia()
j.include("inc.jl")
assert j.increment(1) == 2