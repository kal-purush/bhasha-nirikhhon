#重点是如何实现广度以及深度搜索  
'''
用邻接矩阵：特点，稀疏 表示简单


邻接表实现：高效，使用字典实现
'''

#我们将创建两个类（见 Listing 1和 Listing 2），
#Graph（保存顶点的主列表）和 Vertex（将表示图中的每个顶点）
class Graph:
	#初始化创建一个新的图
	def __init__(self):
		self.vertlist={} #初始化
		self.numVertices=0 #顶点个数
	#向图中添加一个新的顶点
	def addVertex(self,vert):
		self.numVertices=self.numVertices+1 #顶点个数加1
		newVert=Vertex(key)
		self.vertlist[key]=newVert
		return newVert
	#添加一条新的有向边
	def addEdge(self,fromVert,toVert):
	 pass
	def addEdge(self,fromVert,toVert,weight):
		#当添加有向边的顶点不存在的时候，创建
		if fromVert not in self.vertlist:
			nv=self.addVertex(fromVert)
		if toVert not in self.vertlist:
			nv=self.addVertex(toVert)
		self.vertlist[f].addNeighbor(self.vertlist[toVert],weight)
	#找到名为vertKey的顶点
	def getVert(self,vertkey):
		if vertkey in self.vertlist:
			return self.vertlist[vertkey]
		else:
			return None
class Vertex:
	def __init__(self,key):
		self.id=key
		self.connectTo={}
	def addNeighbor(self,nbr,weight):
		self.connectTo[nbr]=weight
	#返回邻接表中与该节点所连接的所有顶点
	def getConnections(self):
		return self.connectTo.keys()
	def getId(self):
        return
	def getweigt(self):
		return self.connectTo[nbr]