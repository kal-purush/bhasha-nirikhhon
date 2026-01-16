#!/usr/bin/python
# exec(open("binpacking.py").read())

import numpy as np
from gurobipy import *

# create a list of n Items to be packed
n = 40  # number of Items
m = 6   # number of Bins
ell = 6 # number of Scenarios
hell = 3
Items = range(n)       # these are the set of objects
Bins = range(m)        # .
Scenarios = range(ell) # .
# The sample distribution of weights:
np.random.seed(1)
wtrain = np.concatenate((0.1 + 0.1*np.random.random( (n,hell) ), 0.2 + 0.2*np.random.random( (n,hell) )), axis = 1)
wtest = np.concatenate((0.1 + 0.1*np.random.random( (n,hell) ), 0.2 + 0.2*np.random.random( (n,hell) )), axis = 1)


#wtrain1 = 0.2 + 0.2*np.random.random( (int(0.5*n),ell) )
#wtrain2 = 0.3 + 0.2*np.random.random( (int(0.5*n),ell) )
#wtrain = np.r_[wtrain1,wtrain2]
#wtest1 = 0.2 + 0.2*np.random.random( (int(0.5*n),ell) )
#wtest2 = 0.3 + 0.2*np.random.random( (int(0.5*n),ell) )
#wtest = np.r_[wtest1,wtest2]

c = 1 # Capacity of the bins (sam for all)

# Create initial model
m = Model("binpacking")
# The variables used, x for assignment in first stage
x = m.addVars(Items, Bins, vtype=GRB.BINARY, name = "X")
# Second stage decision, variables, y cancel and z reassign
y = m.addVars(Items, Bins, Scenarios, vtype=GRB.BINARY, name = "Y")
z = m.addVars(Items, Bins, Scenarios, vtype=GRB.BINARY, name = "Z")
w = m.addVars(Items, Bins, vtype=GRB.BINARY, name = "W")
# upper bounds to be optimized or assigned using constraints below
zupper = m.addVar(vtype=GRB.CONTINUOUS, lb = 0, ub = 100, name = "zupper")
yupper = m.addVar(vtype=GRB.CONTINUOUS, lb = 0, ub = 100, name = "yupper")
# this variable is for the true cancellations sum or y  minus sum of z
yzupper = m.addVar(vtype=GRB.CONTINUOUS, lb = 0, ub = 100, name = "yzupper")

# The objective is to minimize the total number of items packed, yzupper should be zero!
m.setObjective( (quicksum(x[i,b] for i in Items for b in Bins) - 100*yzupper - yupper - zupper - 0.1*quicksum(w[i,b] for i in Items for b in Bins)), GRB.MAXIMIZE)
# Experimental: Set the number of jobs to assign and search for the one with different values for sum Z
fixsumx = m.addConstr( (quicksum(x[i,b] for i in Items for b in Bins) == 14), name = "sumX")

# You can only assign an item once to a bin
onlyonce_constraint = m.addConstrs((quicksum(x[i,b] for b in Bins) <= 1 for i in Items), name = "onlyassignonce")
# Each bin has some capacity c which must be satisfied, you can cancel an assignment (y) and move it (z)
capacity_constraint = m.addConstrs((quicksum(wtrain[i,s]*(x[i,b]-y[i,b,s]+z[i,b,s]) for i in Items) <= c for b in Bins for s in Scenarios), name="capacity")
# you can't cancel an assignment that has not been assigned to a bin b
cancel_condition = m.addConstrs((y[i,b,s] <= x[i,b] for i in Items for b in Bins for s in Scenarios), name = "CancelCondition")
# you can't move an assignment unless it has been cancelled
move_condition = m.addConstrs( (quicksum(z[i,b,s] for b in Bins) <= quicksum(y[i,b,s] for b in Bins) for i in Items for s in Scenarios), name = "MoveCondition")
# lets force an upper bound on how many are moved
move_bound = m.addConstr( (quicksum(z[i,b,s] for i in Items for b in Bins for s in Scenarios) <= zupper), name = "zupperCondition")
# Number of cancelled
cancel_bound = m.addConstr( (quicksum(y[i,b,s] for i in Items for b in Bins for s in Scenarios) <= yupper), name = "yupperConditon")
# The actual number of cancelations is
cancelled = m.addConstr( (quicksum(y[i,b,s] for i in Items for b in Bins for s in Scenarios) - quicksum(z[i,b,s] for i in Items for b in Bins for s in Scenarios) <= yzupper), name = "yzConditon")
# Flexible upper bound on number of modifications
#upper_bound_zupper = m.addConstr( yupper == 50, name = "upper_bound_yupper")
#upper_bound_yupper = m.addConstr( zupper == 50, name = "upper_bound_zupper")

whichiteminbin = m.addConstrs( (y[i,b,s] <= w[i,b] * 10000.0 for i in Items for b in Bins for s in Scenarios))
onlyoneiteminbin = m.addConstrs( (quicksum(w[i,b] for i in Items) <= 1 for b in Bins) )


# Optimize / set TimeLimit
m.setParam('TimeLimit', 3*60)
m.optimize()

# Display the Solutions
m.printAttr("x")
solutionX = m.getAttr('x', x)

# Now for the testing phase:
m.addConstrs((x[i,b] == 0 for i in Items for b in Bins if solutionX[i,b] == 0), name='fix0')
m.addConstrs((x[i,b] == 1 for i in Items for b in Bins if solutionX[i,b] == 1), name='fix1')

# Now for the testing phase:
m.addConstrs((w[i,b] == 0 for i in Items for b in Bins if w[i,b].X == 0), name='fix0w')
m.addConstrs((w[i,b] == 1 for i in Items for b in Bins if w[i,b].X == 1), name='fix1w')
#
m.remove(whichiteminbin)

# Update the weights using testing Data
m.remove(capacity_constraint)
capacity_constraint_testing = m.addConstrs((quicksum(wtest[i,s]*(x[i,b]-y[i,b,s]+z[i,b,s]) for i in Items) <= c for b in Bins for s in Scenarios), name="capacityadd")
#m.remove(upper_bound_zupper)
#m.remove(upper_bound_yupper)
m.setObjective(zupper + yupper + 100.0*yzupper, GRB.MINIMIZE)

m.optimize()
m.printAttr("x")

statscount = 0
statsmincount = 0
numberofitemsinbin = 0
numberofbins = 0
for s in Scenarios:
    print("Scenario",s)
    for b in Bins:
        lebin = []
        lebinremove = []
        print("Bin", b)
        binchanged = 0
        for i in Items:
            if (x[i,b].X == 1):
                lebin = np.append(lebin, wtest[i,s])
                if (y[i,b,s].X == 1):
                    lebinremove = np.append(lebinremove, 1)
                    binchanged = 1
                else:
                    lebinremove = np.append(lebinremove, 0)
        numberofitemsinbin = numberofitemsinbin + len(lebin)
        numberofbins = numberofbins + 1
        print(lebin)
        if (binchanged == 1):
            statscount = statscount + 1
            if (np.argmin(lebin) == np.argmax(lebinremove)):
                statsmincount = statsmincount + 1

print("percetange of time we select the smallest to move", 100*statsmincount/statscount)

print("probability of choosing the smallest bin is", numberofbins/numberofitemsinbin)