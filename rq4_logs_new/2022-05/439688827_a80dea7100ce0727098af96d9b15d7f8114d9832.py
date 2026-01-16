import random
import sys

lines = []

print("Entrez les phrases : ")
for line in sys.stdin:
    if "END" == line.rstrip() or "end" == line.rstrip():
        break
    lines.append(line)

random.shuffle(lines, random.random)

print("----------------------------------------------------")

for line in lines:
    print(line)