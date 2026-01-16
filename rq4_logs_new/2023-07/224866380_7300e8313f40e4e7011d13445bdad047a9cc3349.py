
with open('input.txt') as f:
  lines = f.read().strip().split('\n')

lines = list(map(list, lines))

for x in range(len(lines)):
  for y in range(len(lines[x])):
    lines[x][y] = ((x, y), int(lines[x][y]))

lr = lines
ud = list(map(list, zip(*lr)))

visible = set()
def num_increasing(trees: list) -> int:
  num = 0
  last = -1
  for t in trees:
    if t[1] > last:
      num += 1
      last = t[1]
      visible.add(t[0])
  return num

def reverse(s: list):
  return list(reversed(s))
num = 0
for l in lr + ud + list(map(reverse, lr)) + list(map(reverse, ud)):
  num += num_increasing(l)
print(len(visible))