class BinaryTree:
	def __init__(self,root):
		self.key=root
		self.leftchild=None
		self.rightchild=None
	#插入左子树
	def insertleft(self,newnode):
		if self.leftchild==None:
			self.leftchild=BinaryTree(newnode)
		#当左子树非空，插入新的节点，把现有节点移到下一层
		else:
			t=BinaryTree(newnode)
			t.leftchild=self.leftchild
			self.leftchild=t
	#插入右子树
	def insertright(self,newnode):
		if self.rightchild==None:
			self.rightchild=BinaryTree(newnode)
		else:
			t=BinaryTree(newnode)
			t.rightchild=self.rightchild
			self.rightchild=t
	def getRightchild(self):
		return self.rightchild
	def getLeftchild(self):
		return self.leftchild
	def setRootVal(self,obj):
		self.key=obj
	def getRootval(self):
		return self.key
r = BinaryTree('a')
print(r.getRootval())
print(r.getLeftchild())
r.insertleft('b')
print(r.getLeftchild())
# print(r.getLeftChild().getRootVal())
# r.insertRight('c')
# print(r.getRightChild())
# print(r.getRightChild().getRootVal())
# r.getRightChild().setRootVal('hello')
# print(r.getRightChild().getRootVal())

