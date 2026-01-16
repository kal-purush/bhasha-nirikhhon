from pulp import *
x = LpVariable("x", 0, 3)
y = LpVariable("y", 0, 1)
prob = LpProblem("myProblem", LpMinimize)
prob += x + y <= 2
l = [x, y]
prob += sum(l)
status = prob.solve()
print("s = %s" % LpStatus[status])
print("x = %s" % value(x))
print("y = %s" % value(y))