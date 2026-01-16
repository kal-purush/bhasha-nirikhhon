from graph import Graph, Vertex, Networkx, Graph_tool
import networkx as nx
import matplotlib.pyplot as plt


G = nx.karate_club_graph()
nx.draw(G, with_labels=True)
plt.show()

vertices = []
for e in G.edges():
    vertices.append(Vertex(str(e[0]), [' '], [str(e[1])]))

g = Graph_tool(vertices=vertices)
#g = Networkx(vertices=vertices)
g.draw()

embedding = g.deep_walk()
clusters = g.k_means(elbow_range=(2,30))

for k,v in clusters.items():
    print(k,':',v)