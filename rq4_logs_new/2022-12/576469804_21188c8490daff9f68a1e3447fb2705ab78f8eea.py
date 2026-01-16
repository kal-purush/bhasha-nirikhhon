def run(lines, value, loop, cycles=[], buffer="", count=None):
    pLoop, nLoop = loop-1, loop+1
    if (loop-20) % 40 == 0:
        cycles.append(value * loop)

    if pLoop and pLoop % 40 == 0:
        print(buffer)
        buffer = ""
        
    if pLoop-((pLoop) // 40)*40 in list(range(value-1, value+2)): buffer += "#"
    else: buffer += " "
    
    if count: return run(lines[1:], value + count, nLoop, cycles, buffer)
    elif len(lines) == 0: return sum(cycles)
    else:
        command = lines[0].split()
        if len(command) == 1:
            return run(lines[1:], value, nLoop, cycles, buffer)
        else:
            return run(lines, value, nLoop, cycles, buffer, int(command[1]))

def main(sample):
    filename = "./{0}/{1}.txt".format(__file__.split('\\')[-2], "sample" if sample else "input" )
    with open(filename) as f:
        lines = f.read().splitlines()
        print('Part 2')
        print('Part 1', run(lines, 1, 1, []))

main(True)
print("-"*20)
main(False)