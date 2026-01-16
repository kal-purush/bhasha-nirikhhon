def sequence_class(immutable):
	if immutable:
		klass = tuple
	else:
		klass = list
	return klass


def sequence_class_take_2(immutable):
	return tuple if immutable else list

seq = sequence_class(immutable=True)

t = seq('Hello Kitty')

print(t, type(t))

seq2 = sequence_class_take_2(immutable=True)
s = seq2('Zebrafish')

print(s, type(s))


def print_args(arg1, arg2, *arg):
	print(arg1)
	print(arg2)
	print(arg)


t = (11, 12, 13, 14, 15, 16, 17)

print(*t)


def colors(red, blue, green, **kwargs):
	print("red =", red)
	print("blue =", blue)
	print("green =", green)
	print(kwargs)


k = {'red':22, 'blue':77, 'green': 133, 'alpha': 60}

colors(**k)
