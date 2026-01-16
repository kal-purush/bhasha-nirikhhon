# Binary Search Tree

def less_than(x, y):
	return x < y

class BSTNode:
	def __init__(self, key, left=None, right=None, parent=None):
		self.key = key
		self.left = left
		self.right = right
		self.parent = parent

class BinarySearchTree:
	def __init__(self, root=None, less=less_than):
		self.root = root
		self.less = less
		self.parents = True

	def tree_min(self, n):
		while (n.left != None):
			n = n.left
		return n

	def tree_max(self, n):
		while (n.right != None):
			n = n.right
		return n

	def insert(self, k):
		knode = BSTNode(k)
		p = None
		r = self.root
		while (r != None):
			p = r
			if (self.less(knode.key, r.key)):
				r = r.left
			else:
				r = r.right
		knode.parent = p
		if (p == None):
			self.root = knode
		elif (self.less(knode.key, p.key)):
			p.left = knode
		else:
			p.right = knode
		return knode

	def successor(self, n):
		if (n.right != None):
			return self.tree_min(n.right)
		else:
			p = n.parent
			while ((p != None) and (n == p.right)):
				n = p
				p = p.parent
			return p

	def predecessor(self, n):
		if (n.left != None):
			return self.tree_max(n.left)
		p = n.parent
		while ((p != None) and (n == p.left)):
			n = p
			p = p.parent
		return p

	def search(self, k):
		r = self.root
		while ((r != None) and (k != r.key)):
			if (k < r.key):
				r = r.left
			else:
				r = r.right
		return r

	def transplant(self, x, y):
		if (x.parent == None):
			self.root = y
		elif (x == x.parent.left):
			x.parent.left = y
		else:
			x.parent.right = y
		if (y != None):
			y.parent = x.parent

	def delete_node(self, n):
		if (n.left == None):
			self.transplant(n, n.right)
		elif (n.right == None):
			self.transplant(n, n.left)
		else:
			replace = self.tree_min(n.right)
			if (replace.parent != n):
				self.transplant(replace, replace.right)
				replace.right = n.right
				replace.right.parent = replace
			self.transplant(n, replace)
			replace.left = n.left
			replace.left.parent = replace
		return n

#========================================
#TESTING

if __name__ == "__main__":
	t = BinarySearchTree()
	t.insert(10)
	t.insert(5)
	t.insert(15)
	t.insert(16)
	t.insert(7)
	t.insert(11)
	t.insert(1)
	t.insert(2)
	t.insert(20)
	assert(t.root.key == 10)
	assert(t.root.left.right.key == 7)
	assert(t.root.left.left.right.key == 2)
	assert(t.root.right.left.key == 11)
	assert(t.root.right.right.left == None)
	assert(t.root.right.key == 15)
	assert((t.successor(t.root)).key == 11)
	assert((t.predecessor(t.root)).key == 7)
	assert((t.successor(t.root.left.right)).key == 10)
	assert((t.predecessor(t.root.left.left.right)).key == 1)
	assert((t.search(20)).key == 20)
	assert((t.search(3)) == None)
	t.delete_node(t.root.left.left.right) #deleting node with key 2
	assert(t.root.left.left.right == None)
	t.delete_node(t.root.right) #deleting node with key 15
	assert(t.root.right.key == 16)
	assert(t.root.right.left.key == 11)
	assert(t.root.right.right.key == 20)