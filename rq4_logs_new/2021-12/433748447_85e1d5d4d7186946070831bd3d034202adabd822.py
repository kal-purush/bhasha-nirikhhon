depth = 0
position = 0
f = open("day2.txt", "r")
lines = f.readlines()
for line in lines:
  direction, value = line.split(" ")
  value = int(value)
  print(f'{direction}-{value}: depth {depth} position {position}')
  if direction == 'forward':
    position += value
  elif direction == 'up':
    depth = 0 if depth - value < 0 else depth - value
  elif direction == 'down':
    depth += value
print(depth*position)