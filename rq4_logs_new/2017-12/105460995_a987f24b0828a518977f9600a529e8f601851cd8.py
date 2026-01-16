from nltk.corpus import wordnet as wn
import networkx as nx
import matplotlib.pyplot as plt

Vehicle = wn.synsets('vehicle')[0]

# set of tuples (synset, distances)
first_branch = Vehicle.hypernym_distances()
second_brach = [i[0].hypernym_distances() for i in first_branch]

# create an object Graph
G = nx.Graph()

# Define Nodes and Origin(s) (first and second generations)
first_Nodes = [j[0].name() for j in first_branch]
first_Origin = [j[0].name() for j in first_branch if j[1] == 0]

second_Nodes = []
for n in second_brach:
    for m in n:
        second_Nodes.append(m[0].name())

second_Origins = []
for n in second_brach:
    for m in n:
        if m[1] == 0:
            second_Origins.append(m[0].name())

# Define the edges