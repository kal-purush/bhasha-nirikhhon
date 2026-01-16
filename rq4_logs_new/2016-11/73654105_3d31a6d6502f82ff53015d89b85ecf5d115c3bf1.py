import random as rand

toGuess = 42
popSize = 20
mutrate = 0.25
maxint = 128
maxgens = 100


def bitfield(n):
    return [1 if digit == "1" else 0 for digit in bin(n)[2:]]

def decode(value):
    for i in range(7 - len(value)):
        value.insert(0, 0)
    return shifting(value)

def shifting(bitlist):
    out = 0
    for bit in bitlist:
        out = (out << 1) | bit
    return out

def fitness(value):
    x = decode(value)
    return abs((x - toGuess) / toGuess)

def generate_indv():
    val = rand.randint(0, maxint)
    indv = bitfield(val)
    for i in range(7 - len(indv)):
        indv.insert(0, 0)
    return indv

def generatepopulation(size):
    return [generate_indv() for x in range(size)]


def mutate(child_val):
    field = child_val
    if len(field) <7:
        for i in range(7 - len(field)):
            field.insert(0, 0)
    tochange = rand.randint(0, 6)
    if field[tochange] == 1:
        field[tochange] = 0
    else:
        field[tochange] = 1
    return field


def crossover(parents):
    children = []
    p1v = parents[0]
    for p2v in iter(parents[1:]):
        mutspot = rand.randint(0, 7)
        child_val = p1v[:mutspot] + p2v[mutspot:]
        if rand.randint(0, 100) <= mutrate * 100:
            child_val = mutate(child_val)
        children.append(child_val)
        p1v = p2v
    children.insert(0, parents[0]) #elitism
    return children


def main():
    rand.seed()
    while(1):
        try:
            global toGuess
            toGuess = int(input("Number to guess between 0 and 127: "))
            break
        except Exception:
            print("Invalid input, try again")
    # generate random population
    pop = generatepopulation(popSize)

    # loop
    done = 0
    gen = 0

    # prime the population as sorted by fitness
    pop = sorted(pop, key=fitness)

    # run GA
    while (done != 1 and gen <= maxgens):

        # crossover/mutation and update population
        pop = crossover(pop)

        # sort by fitness
        pop = sorted(pop, key=fitness)

        # if most fit is correct, finish up
        print("GENERATION " + str(gen))
        print("\tBEST SO FAR = " + str(decode(pop[0])))
        if (fitness(pop[0]) == 0.0):
            print("FINISHED: Best individual = " + str(pop[0]) + " = " + str(decode(pop[0])))
            done = 1
        gen += 1

    if done is 0:
        print("the GA's best guess was " + str(decode(pop[0])))

# end loop
if __name__ == "__main__":
    main()