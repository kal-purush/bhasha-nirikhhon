# Extended Euclidean algorithm implementation

import gmpy2

def eea(a, b):
	r, r1 = a, b
	s, s1 = 1, 0
	t, t1 = 0, 1
	while r1 != 0:
		q = gmpy2.div(r, r1)
		r2 = gmpy2.f_mod(r, r1)
		r, s, t, r1, s1, t1 = r1, s1, t1, r2, gmpy2.sub(s, gmpy2.mul(s1, q)), gmpy2.sub(t, gmpy2.mul(t1, q))
	d = r
	return d, s, t