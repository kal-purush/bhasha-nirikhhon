L = []
num_of_commands = input()
for i in range(num_of_commands):
  rawinput = raw_input().split()
  command = rawinput[0]
  inp = rawinput[1:]
      if command == "append":
        inp = eval(inp[0])
        L.append(inp)
      elif command == "extend":
        inp = [eval(i) for i in inp]
        L.extend(inp)
      elif command == "insert":
        index,value = map(eval, inp)
        L.insert(index, value)
      elif command == "remove":
        inp = eval(inp[0])
        L.remove(inp)
      elif command == "index":
        inp = eval(inp[0])
        L.index(inp)
      elif command == "count":
        inp = eval(inp[0])
        L.count(inp)
      elif command == "sort":
        L.sort()
      elif command == "reverse":
        L.reverse()
      elif command == "pop":
        L.pop()
      elif command == "print":
        print L