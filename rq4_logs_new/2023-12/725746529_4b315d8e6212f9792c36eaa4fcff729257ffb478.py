def run():
	task1()
	task2()

from collections import deque

def task1():
	u = []
	with open("inputs/day11.txt", "r") as inp:
		for line in inp:
			u.append(list(line.strip()))
			if line.count("#") == 0:
				u.append(list(line.strip()))
	cols = []
	for i in range(len(u[0])):
		empty = True
		for j in range(len(u)):
			if u[j][i] == "#":
				empty = False
				break
		if empty:
			cols.append(i)
	for y in u:
		i = 0
		for col in cols:
			y.insert(col+i, ".")
			i+=1
	gs = set()
	for y in range(len(u)):
		for x in range(len(u[y])):
			if u[y][x] == "#":
				gs.add((x, y))
	out = 0
	for g in gs:
		x, y = g
		for g2 in gs:
			if g2 != g:
				x2, y2 = g2
				out += abs(x2-x) + abs(y2-y)
	print(out//2)


def task2():
	u = []
	rows = set()
	with open("inputs/day11.txt", "r") as inp:
		i = 0
		for line in inp:
			u.append(list(line.strip()))
			if line.count("#") == 0:
				rows.add(i)
			i += 1
	cols = set()
	for i in range(len(u[0])):
		empty = True
		for j in range(len(u)):
			if u[j][i] == "#":
				empty = False
				break
		if empty:
			cols.add(i)
	gs = set()
	for y in range(len(u)):
		for x in range(len(u[y])):
			if u[y][x] == "#":
				gs.add((x, y))
	out = 0
	for g in gs:
		x, y = g
		for g2 in gs:
			if g2 != g:
				x2, y2 = g2
				for i in range(min(x,x2),max(x,x2)):
					if i in cols:
						out += 1000000
					else:
						out += 1
				for i in range(min(y,y2),max(y,y2)):
					if i in rows:
						out += 1000000
					else:
						out += 1
	print(out//2)

if __name__=='__main__':
	run()