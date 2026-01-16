n_r = 161 #18, 30, 110, 161

n_bin = bin(n_r)[2:]
n_bin = "0"*(8-len(n_bin)) + n_bin

PATTERNS = [("1", "1", "1"), ("1", "1", "0"), ("1", "0", "1"), ("1", "0", "0"), 
            ("0", "1", "1"), ("0", "1", "0"), ("0", "0", "1"), ("0", "0", "0")]

rules = {pattern: rule for pattern, rule in zip(PATTERNS, n_bin)}
print(rules)

def generate(field):
	new_field = ["0"]*len(field)

	for i, ch in enumerate(field):
		if 0 < i < len(field) - 1:
			tmp = tuple(field[i-1:i+2])
			new_field[i] = rules[tmp]

	return new_field
