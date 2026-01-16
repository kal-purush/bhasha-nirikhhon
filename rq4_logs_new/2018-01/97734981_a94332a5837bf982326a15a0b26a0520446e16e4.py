class Vertex(object):
  
  def __init__(self,key):
    self.id = key
    self.connectedTo = {}
    
  def addNeighbor(self,nbr,weight=0):
    self.connectedTo[nbr] = weight
    
  def getConnections(self):
    return self.connectedTo.keys()
  
  def getId(self):
    return self.id
  
  def getWeight(self,nbr):
    return self.connectedTo[nbr]
    
  def __str__(self):
    return str(id) + "connected to " + str([x.id  for x in self.connectedTo])
    
class Graph(object):
  
  def __init__(self):
    self.vertList = {}
    self.numVertices = 0
    
  def addVertex(self,key):
    self.numVertices += 1 
    new_vert = Vertex(key)
    self.vertList[key] = new_vert
    return new_vert
    
  def getVertex(self,n):
    if n in self.vertList:
      return self.vertList[n]
    else:
      return None
      
  def __contains__(self,n):
    return n in self.vertList

  def addEdge(self,fromv,tov,cost=0):
    if fromv not in self.vertList:
      nv = self.addVertex(fromv)
    if tov not in self.vertList:
      nv = self.addVertex(tov)
    self.vertList[fromv].addNeighbor(self.vertList[tov], cost)

  def getVertices(self):
    return self.vertList.keys()

  def __iter__(self):
    return iter(self.vertList.values())
    
    
    
g = Graph()
for i in range(6):
  g.addVertex(i)
print(g.vertList)

g.addEdge(0,1,2)


for vertex in g:
  print(vertex)
  print(vertex.getConnections())
  print('\n')


