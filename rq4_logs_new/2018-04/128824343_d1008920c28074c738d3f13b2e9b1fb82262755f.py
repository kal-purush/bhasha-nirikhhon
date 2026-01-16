import itertools

out = ['000', '001', '010', '011', '100', '101', '111', '110']


def CNOT(bit, control_bit, flip_bit):
	bitl = list(bit)
	if bitl[control_bit] == "1":
		if bitl[flip_bit] == "0":
			bitl[flip_bit] = "1"
		else:
			bitl[flip_bit] = "0"
	return "".join(bitl)


def apply(bit_list, actions):
	out = []
	for each in bit_list:
		out.append(CNOT(each, int(actions[0]), int(actions[1])))
	return out

def generate_bit_list():
	return ["".join(entry) for entry in list(itertools.product("01",repeat=3))]


def recurse(x, n):
	if n == 0:
		print x
		if x == out:
			"***yay***"
		return
	for i in range(n):
		for action in list(itertools.permutations("012", 2)):
			y = apply(x, action)
			recurse(y, n-1)

if __name__ == "__main__":
	recurse(generate_bit_list(), 10)