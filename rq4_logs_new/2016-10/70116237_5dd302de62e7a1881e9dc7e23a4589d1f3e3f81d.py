# Huffman implemenation by Manuel Tobar
# Semestre 2016-2
#
# ToDo: add unit tests

from heapq import heappush, heappop
import unittest

# Class Node for Heap
class Node:
    def __init__(self, symbol = '', freq = 0):
        self.symbol = symbol
        self.freq = freq
        self.right = None
        self.left = None
    def __lt__(self, other):
        return self.freq < other.freq

def Huffman_compression(nodes):
    n = len(nodes)
    heap = []
    for node in nodes:
        heappush(heap,node)
    for _ in range(1, n):
        z = Node()
        z.left = heappop(heap)
        z.right = heappop(heap)
        z.freq = z.left.freq + z.right.freq
        heappush(heap, z)
    return heap[0]

def Huffman_transform(data):
    n = len(data)
    uniq_arr = dict()
    for number in data:
        if uniq_arr.get(number) == None:
            uniq_arr.update({number : Node(number, 1 / n)})
        else:
            a = uniq_arr[number]
            a.freq = ((a.freq * n) + 1) / n
    values = uniq_arr.values()
    arr = []
    for val in values:
        arr.append(val)
    return arr

def Huffman(array):
    """Toma un sets de datos y los comprime usando el algoritmo de Huffman

    *array*, es pasado a la funcion 'Huffman_transform' y este retorna un
    arreglo de nodos para ser comprimidos. Luego es enviado a 'Huffman_Compression'
    que retorna el arbol binario con los datos comprimidos."""
    array = Huffman_transform(array)
    return Huffman_compression(array)

class TestHuffman(unittest.TestCase):
    def test(self):
        a = [1, 0, 0, 1, 1, 0, -1, -2, -3, -3, -4, -4, -4, -4, -3]
        b =Huffman(a)
        inorder(b)
        #a way to check if nodes are good

def inorder(node):
    if node == None:
        return
    inorder(node.left)
    # if node.symbol != '':
    print(node.symbol, node.freq)
    inorder(node.right)

if __name__ == '__main__':
    from sys import argv, exit

    if len(argv) == 1:
        unittest.main()
        exit()

    filename = argv[1]

    A = [] # creates an empty array
    with open(filename, 'r') as f:
        for line in f:
            A.append(int(line))
    print(A)
    inorder(Huffman(A))