import random

FILENAME = 'big.in'
OUT = 'big.out'

#Parametros a ajustar si quieren cambiarle la presicion y la cantidad de operaciones
MAX_OPS = 200000
MIN_PREC = 10
MAX_PREC = 100

OPS = ['+', '*', '-']

def rand_int(n_digits):
	n = random.randint(1, 10**n_digits - 1)
	return -n if random.randint(0,1) > 0 else n

def eval_op(o1, o2, op):
	if op == '+':
		return o1 + o2
	if op == '*':
		return o1 * o2
	if op == '-':
		return o1 - o2

if __name__ == '__main__':
	prec = random.randint(MIN_PREC, MAX_PREC)
	print 'Using precision = %d' % prec
	with open(FILENAME, 'w') as f:
		with open(OUT, 'w') as w:
			n = random.randint(1, MAX_OPS)
			for _ in xrange(n):
				o1 = rand_int(prec)
				o2 = rand_int(prec)
				op = random.choice(OPS)
				line = '%d%s%d\n' % (o1, op, o2)
				outline = '%d\n' % eval_op(o1,o2,op)
				f.write(line)
				w.write(outline)
		f.write("#calculate")
	w.close()
	f.close()