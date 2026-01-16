from grafo import Grafo
from funciones_grafos import bfs
from collections import deque

class NetStats():
    
    def __init__(self):
    	self.grafo = Grafo()
        #self.comandos = ["camino"]

    def agregarArticulo(self, titulo):
    	self.grafo.agregarVertice(titulo)

    def agregarLink(self, titulo1, titulo2):
    	self.grafo.agregarArista(titulo1, titulo2)

    def camino(self, origen, destino):
    	origenes, cantidad_links = bfs(self.grafo, origen)
    	q = deque()

    	v = destino
    	while v!= origen:
    		q.append(v)
    		v = origenes[v]
    	self.imprimirCamino(origen, q, cantidad_links[destino])

    def imprimirCamino(self, origen, q, costo):
    	print(origen, end=' ')

    	while q:
    		print ("-> " + q.pop(), end=' ')

    	print()
    	print ("Costo:", end=' ')
    	print (costo)