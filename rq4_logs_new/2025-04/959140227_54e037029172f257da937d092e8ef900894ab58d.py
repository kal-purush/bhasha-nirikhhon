# a = free points
# b = third collinear points
# c = fourth collinear points
# d = free lines
# e = lines incident to one point
# f = lines incident to two points
# m = number of cameras
# inf = whether we know (1) the point at infinity or not (0)


# initial equation is big = m*den, where big and den contain no m.
# then write m = big/den = 1 + diff/den, with diff = big-den
# divisibility condition: diff % den == 0.
# in particular, den <= diff.

def big(a, b, c, d, e, f, inf):
    return 3*a + b + c + 4*d + 2*e - 7
    	
def den(a, b, c, d, e, f, inf):
	return inf + 2*a + 2*b + c + 3*d + 2*e + f - 8

# structural conditions are conditions that must be satisfied for the
# tuple (a, b, ..., inf) to make geometric sense. we derived the list of
# structural conditions by inspecting all solutions found by the
# program until we encountered one that doesn't make geometric sense, at
# which point we added a new structural condition to exclude that
# solution. We repeated this process until all solutions made sense.

def structural_condtitions_satisfied(a, b, c, d, e, f, inf):
	# If we have no free points, then there cannot be any other points
	# or lines incident to points.
	if a == 0:
		if (b != 0) or (c != 0) or (e != 0) or (f != 0):
			return False

	# No 3rd collinear points implies no 4th collinear points.
	if (b == 0) and (c != 0):
		return False

	# If we have only one free point, then we cannot have any other
	# point, or lines incident to two points.
	if a == 1:
		if (b != 0) or (c != 0) or (f != 0):
			return False

	# The existence of third collinear points or of any lines means that
	# we know the point at infinity.
	if inf == 0:
		if (c != 0) or (d != 0) or (e != 0) or (f != 0):
			return False
			
	# There has to be room for 'f' lines. Here only coded for a = 2
	# since that's the only case that appears.
	if (a == 2) and (b != 0) and (f != 0): return False
	if (a == 2) and (f > 1): return False
		
	# Exclude empty configuration, even if point at infinity is known.
	# It's balanced if m = 1, but consider it trivial.
	if [a,b,c,d,e,f] == [0,0,0,0,0,0]: return False 
	
	return True

# have to check first that den cannot be zero or negative. if so, then
# necess. that condition also holds for big. exclude those cases.

den_zero_found = False
den_negative_found = False

def check_den_zero_or_negative(a,b,c,d,e,f,inf):
	global den_zero_found, den_negative_found
	big_local = big(a,b,c,d,e,f,inf)
	den_local = den(a,b,c,d,e,f,inf)
	if not structural_condtitions_satisfied(a, b, c, d, e, f, inf):
		return
	if (big_local == 0) and (den_local == 0):
		print(a,b,c,d,e,f,inf)
		den_zero_found = True
	elif (big_local < 0) and (den_local < 0):
		if (big_local % den_local != 0): return
		print(a,b,c,d,e,f,inf)
		den_negative_found = True

for a in range(0,5):
	for b in range(0,5):
		for c in range(0,9):
			for d in range(0,3):
				for e in range(0,5):
					for f in range(0,9):
						for inf in range(0,2):
							check_den_zero_or_negative(a,b,c,d,e,f,inf)

if den_zero_found: print ("Found config with denominator zero.")

if den_negative_found: print ("Found config with negative denominator")
	
def diff(a, b, c, d, e, f, inf):
	return big(a, b, c, d, e, f, inf) - den(a, b, c, d, e, f, inf)
    
# integer inequality: we have 0 >= ineq, i.e. den - diff <= 0.
def ineq(a, b, c, d, e, f, inf):
	return den(a, b, c, d, e, f, inf) - diff(a, b, c, d, e, f, inf)

# build list of balanced problems by checking each tuple for the integer
# equality. The above inequality ineq ensures that the number of tuples
# to check is finite.
res = []

def check_candidate(a, b, c, d, e, f, inf):
	global res
	den_local = den(a, b, c, d, e, f, inf)
	diff_local = diff(a, b, c, d, e, f, inf)
	ineq_local = den_local - diff_local
	if 0 < ineq_local: return
	if not structural_condtitions_satisfied(a, b, c, d, e, f, inf):
		return
	if den_local <= 0: return
	if diff_local % den_local != 0: return
	m = diff_local//den_local + 1 # // is integer division in python3.
	res += [[a, b, c, d, e, f, m, inf]]


for a in range(0, 10):
	for b in range(0, 4):
		for c in range (0, 10):
			for d in range (0, 5):
				for e in range (0, 4):
					for f in range (0, 10):
						for inf in range (0, 2):
							check_candidate(a, b, c, d, e, f, inf)

# collect statistics on the distribution of the m.
max_m = 0

# calculate maximum m occurring						
for x in res:
	m = x[6]
	if m > max_m: max_m = m
	
m_list = [0] * 9

print()

# list distribution of the m
for x in res:
	m = x[6]
	m_list[m] += 1
	print (x)

print()
print("Solutions found:", len(res))
print("Distribution of m:", m_list)

# now we need to split up the combinatorially ambiguous tuples into
# combinatorially unambiguous ones. Since there are only 3, we could do
# it by hand. but we use instead a semi-automated system.

# represent the additional combinatorial datum gamma (the max number
# of lines adjacent to a point) as alpha + beta, according to which
# lines are 'E' lines (alpha) and which are 'F' lines (beta).

res_models = []

for x in res:
	# alpha = maximal number of 'E' lines adjacent to a point.
	a = x[0]
	if (a == 0):
		alpha = 0
		res_models += [x+[alpha]]
		continue
	e = x[4]
	alpha_low = (e-1)//a + 1 # ceiling of e/a.
	alpha_high = e
	for alpha in range (alpha_low, alpha_high+1):
		res_models += [x+[alpha]]
		
res_models_two = []

for x in res_models:
	# beta = number of 'F' lines adjacent to the first 'A' point
	# in the found solutions, f ranges from 1 to 2.
	a = x[0]
	e = x[4]
	f = x[5]
	beta = 0
	# if f = 2 then one line will go through A_0 A_2.
	if (f >= 2): beta += 1
	if (f >= 1):
		# if a = 2 then one line will go through A_0 A_1.
		if (a == 2):
			beta += 1
			res_models_two += [x+[beta]]
		# if e = 0, then the 'A' points are not distinguished.
		# in that case, if a = 3 then we can pick any point to be A_0.
		elif (e == 0) and (a == 3):
			beta += 1
			res_models_two += [x+[beta]]
		# same goes if e = 0 and f = 1.
		elif (e == 0) and (f == 1):
			beta += 1
			res_models_two += [x+[beta]]
		# now an A_0 is distinguished and there are multiple choices
		# to put the remaining line.
		else:
			res_models_two += [x+[beta]]
			beta += 1
			res_models_two += [x+[beta]]
	# last, if f == 0 then there's no lines to put.
	else: res_models_two += [x+[beta]]
	
print()

# print found solutions in Macaulay2 format, summing alpha and beta
# for the simpler combinatorial parameter gamma.
print("parameters = (")
for y in res_models_two:
	print(str((y[0],y[1],y[2],y[3],y[4],y[5],y[6],y[8]+y[9],y[7])) +",")
print(")")
print()
print("Problems found:", len(res_models_two))

# now do the same, but print to a file the final answer to use later.

with open('model-parameters-infty.m2', 'w') as f:
	f.write("parameters = (\n")
	for y in res_models_two:
		f.write(str((y[0],y[1],y[2],y[3],y[4],y[5],y[6],\
			y[8]+y[9],y[7]))+",\n")
	f.write(")\n")






