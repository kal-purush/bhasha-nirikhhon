class Node:

	def __init__(self, data):
		self.data = data
		self.left = None
		self.right = None

class Tree:

	def __init__(self):
		self.root = None

	def insert_node(self, root, node):

		if root == None:
			root = node

		if root.data > node.data:
			if root.left == None:
				root.left = node
			else:
				self.insert_node(root.left, node)

		else:
			if root.data < node.data:
				if root.right == None:
					root.right = node
				else:
					self.insert_node(root.right, node)

	def search(self, data):
		node = self.root
		while node:
			if data < node.data:
				node = node.left
			elif data > node.data:
				node = node.right
			else:
				print node, node.data
				return node
		return "Not found"


a = Node(5)
b = Node(6)
c = Node(7)
d = Node(8)
e = Node(4)
f = Node(5)

tree = Tree()

tree.root = a

tree.insert_node(a, b)
tree.insert_node(a, c)
tree.insert_node(a, d)
tree.insert_node(a, e)
tree.insert_node(a, f)

tree.search(8)

