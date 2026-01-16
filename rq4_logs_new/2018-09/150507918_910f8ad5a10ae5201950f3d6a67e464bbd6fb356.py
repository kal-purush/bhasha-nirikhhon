#使用二叉堆(binary heap)实现优先队列（prior queue）
#完全二叉树的一个优点 父节点与子节点的位置关系很好
class Binheap:
	def __init__(self):
		self.heaplist=[0]
		self.currentsize=0
	#新插入的项可能比较小，为了维持堆的大小特性，需要把新插入的项与父节点交换
	def preup(self,i):
		while i//2>0:
			if self.heaplist[i]<self.heaplist[i//2]:
				self.heaplist[i//2],self.heaplist[i]=self.heaplist[i],self.heaplist[i//2]
			i=i//2
	def insert(self,k):
		self.heaplist.append(k)
		self.currentsize=self.currentsize+1
		preup(self.currentsize)
		
	def predown(self,i):
		while (i*2)<=self.currentsize:
			mc=self.minchild(i)
			if self.heaplist[i]>self.heaplist[mc]:
				self.heaplist[i],self.heaplist[mc]=self.heaplist[mc],self.heaplist[i]
		i=mc
	#辅助函数 帮助找到当前父节点下的最小子节点
	def minchild(self,i):
		#只有一个子节点
		if i*2+1>self.currentsize:
			return i*2
		else: #，有两个子节点比较左右子节点
			if self.heaplist[i*2]<self.heaplist[i*2+1]:
				return i*2
			else:
				return i*2+1
	def delmin(self):
		
		retval=self.heaplist[1] #当前heap 中的最小值 heaplist[0]=0
		self.heaplist[i]=self.heaplist[currentsize]
		self.currentsize=self.currentsize-1
		self.heaplist.pop()
		self.predown(1)
		return retval


