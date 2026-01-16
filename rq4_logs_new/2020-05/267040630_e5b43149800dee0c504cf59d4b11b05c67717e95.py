class node:

	def __init__(self,data):
		self.data = data
		self.left = None
		self.right = None

	def inser_into_tree(self,data):

			if data > self.data:
				if self.right == None:
					self.right = node(data)
				else:
					self.right.inser_into_tree(data)

			else:
				if self.left == None:
					self.left = node(data)
				else:
					self.left.inser_into_tree(data)

	def print_tree_prefix(self):

		if self.left:
			self.left.print_tree_prefix()

		print(self.data)

		if self.right:
			self.right.print_tree_prefix()

	def print_tree_postfix(self):

		if self.right:
			self.right.print_tree_postfix()

		print(self.data)

		if self.left:
			self.left.print_tree_postfix()

root = node(10)
root.inser_into_tree(20)
root.inser_into_tree(5)
root.inser_into_tree(15)

root.print_tree_prefix()
root.print_tree_postfix()
