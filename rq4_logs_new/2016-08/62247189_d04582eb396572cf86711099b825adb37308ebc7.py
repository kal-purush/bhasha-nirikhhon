import math, random

# Determine the half-sample mode (0.5 breakdown point estimate of the mode)
# See "On a Fast, Robust Estimator of the Mode", Bickel et al.
def half_sample_mode_sorted(x):
	# x is the input array. It must be sorted in ascending order.
	# N = length(x), x contains x_1...x_N

	# if N = 1, return x_1
	# if N = 2, return (x_1+x_2)/2.0

	# Otherwise, find i, k, so that k-i is the least integer greater
	# than N/2, and x_k-x_i is minimized. Recurse on x_i...x_k.
	# (Tiebreak to highest??)

	if len(x) == 1:
		return x[0]

	if len(x) == 2:
		return x[0]/2.0 + x[1]/2.0

	width = int(math.ceil(len(x)/2.0))

	record = float("inf")
	recordholder = -1

	for i in xrange(len(x)-width):
		k = i + width

		sample_range = x[k-1] - x[i]	# Off by one is tricky!

		if sample_range <= record:		# Tiebreak in favor of greatest score
			record = sample_range
			recordholder = i
		
	return half_sample_mode_sorted(x[recordholder:recordholder+width])

def half_sample_mode(x):
	return half_sample_mode_sorted(sorted(x))

# Returns true if the method seems to be monotone, false otherwise.
def simple_monotonicity_check(numiters, n, print_failure=True):
	# There are two stages: first one, we create a test vector.
	# Second one, we try to increment or decrement some number and
	# check if it makes the HSM decrease or increase respectively.
	# To evenly share the load, each loop has close to sqrt(numiters) 
	# iterations for a total close to numiters.
	numiters_outer_stage = int(math.ceil(math.sqrt(numiters)))
	numiters_inner_stage = numiters/numiters_outer_stage

	was_ever_mono_failure = False

	for i in xrange(numiters_outer_stage):
		test_vector = [random.random() for x in xrange(n)]
		original_mode = half_sample_mode(test_vector)

		for j in xrange(numiters_inner_stage):
			k = random.randint(0, n-1)
			orig_number = test_vector[k]
			test_vector[k] = random.random()
			new_mode = half_sample_mode(test_vector)

			mono_failure = \
				(test_vector[k] > orig_number and new_mode < original_mode) or\
				(test_vector[k] < orig_number and new_mode > original_mode)

			was_ever_mono_failure = was_ever_mono_failure or mono_failure

			if mono_failure:
				if print_failure:
					print "Monotonicity failure!"
					print "We changed %.2f into %.2f and then the HSM changed from %.2f to %.2f" % (
						orig_number, test_vector[k], original_mode, new_mode)
				return
		
			test_vector[k] = orig_number

	return (not was_ever_mono_failure)

def mode_ratings(ratings):
	# Ratings is a list of c entries (one per candidate), each of which is a
	# list of v entries (number of voters) giving their ratings. Output is
	# a list of scores, the max being the winner's.

	# Pretty easy.
	return [half_sample_mode(x) for x in ratings]

# The method isn't monotone.
# Not so surprising, given that the mode isn't monotone either!

simple_monotonicity_check(2048, 10)

# Pizza example (http://rangevoting.org/MajCrit.html):

print "Doing pizza example with 9 voters..."

# 5: X=99, Y=50, Z=0
# 4: X=0,  Y=99, Z=10

print mode_ratings([
	[99, 99, 99, 99, 99,  0,  0,  0,  0],	# X
	[50, 50, 50, 50, 50, 99, 99, 99, 99],   # Y
	[ 0,  0,  0,  0,  0, 10, 10, 10, 10]    # Z
	])