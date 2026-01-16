from pyibex import *
from pyinvariant import *

bouee =True
c = 1
def field2():
	S1 = ""
	S2 = ""
	S1 = S1 + "(-x[1]+"+str(c)+"*x[0]/abs(x[0]))/sqrt(x[0]^2 + x[1]^2)"
	S2 = S2 + "(x[0] +"+str(c)+"*x[1]/abs(x[1]))/sqrt(x[0]^2 + x[1]^2)"
	if bouee:   
		for i in range(len(TC1)):
			c1=TC1[i]
			c2=TC2[i]
			r=Tr[i]
			u1=Tu1[i]
			u2=Tu2[i]
			DX1 = "(x[0]-"+str(c1)+")"
			DX2 = "(x[1]-"+str(c2)+")"
			TT = "(" + str(p1) + "*tanh(" + DX1 +"*"+str(u2) + "-" + DX2 +"*" + str(u1) + "))"
			VXI = "(" + str(u1) +"-" + TT +"*"+str(u2) +")"
			VYI=  "(" + str(u2) +"-" + TT +"*"+str(u1)+")"
			A = "exp(-(" + DX1 +"/"+str(r) +")^2"+ "-(" + DX2 +"/"+str(r) +")^2)"
			S1 = S1 +"+"+ A + "*" +VXI
			S2 = S2 + "+" + A + "*" +VYI
	S =  "(" + S1 +"," +S2 +")" 
	return S


ech=17
da=ech*0.07
TC1 = [10,0,-10,0]
TC2 = [0,10, 0,-10]
Tr  = [5,5,5,5]
Tu1 = [0,-1,0,1]
Tu2 = [1,0,-1,0]
p1=1.5 #focus



# Define the search space
space = IntervalVector([[-ech, ech],[-ech,ech]])

# Create the grpah structure
smartSubPaving = SmartSubPaving(space)

# Create the Domain
dom = Domain(smartSubPaving, FULL_DOOR)
dom.set_border_path_in(False)
dom.set_border_path_out(False)

f_sep = Function("x[2]", "(x[0])^2+(x[1])^2-(1.0)^2")
s = SepFwdBwd(f_sep, GEQ) # possible options : LT, LEQ, EQ, GEQ, GT
dom.set_sep(s); # Reduce the domain with a separator

# Create the Dynamics
f = Function("x[2]",field2())
dyn = DynamicsFunction(f, FWD_BWD)

# Create the Maze associated with the Domain and the dynamics
maze = Maze(dom, dyn)

# Contract the system
for i in range(15):
	print(i)
	smartSubPaving.bisect()
	maze.contract()

# Visualization
visu = VibesMaze("Invariant_Ile", maze)
visu.setProperties(0,0,512,512)
visu.show()

