import random
import datetime

"""
https://medium.com/@rinu.gour123/python-genetic-algorithms-with-artificial-intelligence-b8d0c7db60ac
"""

geneSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
target = "IVANJUANKARA"

def gen_parent(length):
    genes = []
    while len(genes) < length:
        sampleSize = min(length-len(genes), len(geneSet))
        genes.extend(random.sample(geneSet, sampleSize))
    return ''.join(genes)

def get_fitness(guest):
    return sum(1 for expected, actual in zip(target, guest) if expected==actual)

def mutate(parent):
    index = random.randrange(0, len(parent))
    childGenes = list(parent)
    newGene, alternate = random.sample(geneSet, 2)
    childGenes[index] = alternate if newGene==childGenes[index] else newGene
    return ''.join(childGenes)

def display(guest):
    timeDiff = datetime.datetime.now()-startTime
    fitness = get_fitness(guest)
    print("{}\t{}\t{}".format(guest, fitness, timeDiff))

if __name__ == "__main__":
    random.seed()
    startTime = datetime.datetime.now()
    bestParent = gen_parent(len(target))
    bestFitness = get_fitness(bestParent)
    display(bestParent)
    while True:
        child = mutate(bestParent)
        childFitnes = get_fitness(child)
        if bestFitness >= childFitnes:
            continue
        display(child)
        if childFitnes >= len(bestParent):
            break
        bestFitness = childFitnes
        bestParent = child
    