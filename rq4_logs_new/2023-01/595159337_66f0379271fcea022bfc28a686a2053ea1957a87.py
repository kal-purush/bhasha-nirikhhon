import numpy as np
from graph import *
from dijkstra1 import *
import numpy as np
from docplex.cp.model import CpoModel


def compute_V_adjmat(graph):
    V = sorted(list(graph.get_nodes()))
    adj_mat = [[0 for _ in range(len(V))] for _ in range(len(V))]

    for i in range(len(V)):
        for adj_id in graph.get_adjacent_nodes(V[i]):
            adj_mat[i][adj_id] = 1
    return V, adj_mat

def compute_edges(V, adj_mat):
    count = 1
    all_edges = []
    name_edge = [[0 for _ in range(len(V))] for _ in range(len(V))]
    for i in range(len(name_edge)):
        for j in range(i+1, len(name_edge[i])):
            if adj_mat[i][j]==1: #ij-edge
                name_edge[i][j] = str(count)
                name_edge[j][i] = str(count)
                all_edges.append(str(count))
                count+=1
    return name_edge, all_edges

def powers_A(adj_mat):
    A = [adj_mat, adj_mat]
    for i in range(len(adj_mat)):
        A.append(np.matmul(A[-1], adj_mat))
    return A

def compute_taille_pcc(V, A):
        
    taille_PCC = [[0 for _ in range(len(V))] for _ in range(len(V))]

    for i in range(len(V)):
        for j in range(len(V)):
            for ind_mat in range(1, len(A)):
                if taille_PCC[i][j] == 0:
                    if A[ind_mat][i][j]==1:
                        taille_PCC[i][j] = ind_mat
                    if A[ind_mat][i][j]>1:
                        taille_PCC[i][j] = -1 #plusieurs PCC

    for i in range(len(V)):
        taille_PCC[i][i] = 0
        for j in range(len(V)):
            if taille_PCC[i][j] == -1:
                taille_PCC[i][j] = 0

    return taille_PCC


def compute_PCC_revPCC(V, graph, taille_PCC, all_edges, name_edge):
    dijkstra = {}
    PCC = [['' for _ in range(len(V))] for _ in range(len(V))]
    for v in V:
        dijkstra[v] = DijkstraSPF(graph, v)

    for i in range(len(V)):
        for j in range(len(V)):
            if taille_PCC[i][j]!=0:
                PCC[i][j] = dijkstra[V[i]].get_edge_path(V[j], name_edge)

    rev_PCC = {edge : [] for edge in all_edges}
    for edge in all_edges:
        for i in range(len(PCC)):
            for j in range(i, len(PCC[i])):
                if edge in PCC[i][j]:
                    rev_PCC[edge].append([i,j])

    return PCC, rev_PCC

def chromatic_number(adj_mat):
  # Adjacent Matrix
  G = adj_mat

  # inisiate the name of node.
  node = [str(i) for i in range(len(G))]
  t_={}
  for i in range(len(G)):
    t_[node[i]] = i

  # count degree of all node.
  degree =[]
  for i in range(len(G)):
    degree.append(sum(G[i]))

  # inisiate the posible color
  colorDict = {}
  for i in range(len(G)):
    colorDict[node[i]]=[i for i in range(1,100)]


  # sort the node depends on the degree
  sortedNode=[]
  indeks = []

  # use selection sort
  for i in range(len(degree)):
    _max = 0
    j = 0
    for j in range(len(degree)):
      if j not in indeks:
        if degree[j] > _max:
          _max = degree[j]
          idx = j
    indeks.append(idx)
    sortedNode.append(node[idx])

  # The main process
  theSolution={}
  for n in sortedNode:
    setTheColor = colorDict[n]
    theSolution[n] = setTheColor[0]
    adjacentNode = G[t_[n]]
    for j in range(len(adjacentNode)):
      if adjacentNode[j]==1 and (setTheColor[0] in colorDict[node[j]]):
        colorDict[node[j]].remove(setTheColor[0])

  maxi = 0
  for t,w in sorted(theSolution.items()):
    maxi = max(w, maxi)

  return maxi
  # Print the solution
  #for t,w in sorted(theSolution.items()):
  #  print("Node",t," = ",w)

def model(adj_mat, all_edges, PCC, rev_PCC):
    N = len(adj_mat)
    M = len(all_edges)

    mdl = CpoModel()

    # 2. Create variables        
    X = [mdl.binary_var(name=f"x_{i}") for i in range(N)] #sommets
    Y = [mdl.binary_var(name=f"y_{j}") for j in range(M)] #arêtes

    #print(N,M)

    obj = sum(X) #+ sum([(1-elem)*1000 for elem in Y])
    mdl.add(mdl.minimize(obj))

    for i in range(N):
        for ip in range(N):
            mdl.add(X[i]*X[ip]*len(PCC[i][ip]) <= sum([Y[int(edge)-1] for edge in PCC[i][ip]]))

    mdl.add(sum(Y)==len(Y))

    for j in range(len(all_edges)):
        mdl.add(sum([X[paire[0]]*X[paire[1]] for paire in rev_PCC[all_edges[j]]]) >= Y[j])

    return mdl