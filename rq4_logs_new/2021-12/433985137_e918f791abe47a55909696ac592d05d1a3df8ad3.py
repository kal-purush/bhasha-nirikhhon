input = []

lists = [[] for i in range(12)]

with open('input.txt') as f:
    for line in f:
        for index, val in enumerate(line.strip()):
            lists[index].append(val)

most_frequent = []

for l in lists:
    most_frequent.append(max(set(l), key = l.count))

temp = ""
temp2 = ""

gamma_rate = temp.join(most_frequent)
opposite = []

for i in range(len(gamma_rate)):
    val = '0' if gamma_rate[i] == '1' else '1'
    opposite.append(val)

epsilon_rate = temp2.join(opposite)

print(int(gamma_rate, 2) * int(epsilon_rate, 2))