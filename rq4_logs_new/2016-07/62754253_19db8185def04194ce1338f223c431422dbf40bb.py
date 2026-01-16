from Node import Node

class DoublyLinkedList(object):

	def __init__(self, head = None, tail = None):
		self.head = head
		self.tail = tail

	#adds new node to end of linked list
	def addNode(self, value):
		node = Node(value)
		#special case: adding first node to empty list
		if self.tail == None:
			self.head = node
			self.tail = node
			node.prev = None
			node.next = None
		else:
			#make former end node the new end node's prev
			node.prev = self.tail 
			self.tail.next = node
			#make new node the end
			self.tail = node
			node.next = None
		return self #allow for chaining

	#prints the data stored in each node of the linked list
	def printAll(self):
		#special case if no nodes:
		if self.head == None:
			print "Empty list"
		else: #loop through all nodes
			current = self.head
			while current.next != None:
				current.printNode()
				current = current.next
			current.printNode()

	#deletes the first found node (search from front) with node.value = value
	#if value isn't found, a message is printed but no error is thrown
	def deleteNode(self, value):
		if self.head == None:
			print "No nodes to delete."

		elif self.head.value == value: #special case, deleting from front
			self.head = self.head.next
			self.head.prev = None

		else: #loop to find value (or end!)
			current = self.head
			while current.next != None and current.value != value:
				current = current.next
			if current.value == value:
				current.prev.next = current.next
				if current.next != None:
					current.next.prev = current.prev
			else:
				print "Couldn't find that value."

		return self #for chaining

	#inserts a node with value BEFORE the first found node with value=before
	#BEFORE is used because we already have method to add to end
	#if before value is not found, no node is inserted
	def insertNode(self, value, before):
		node = Node(value)
		#special case: adding to front
		if self.head.value == before:
			node.next = self.head
			self.head.prev = node
			self.head = node
		else:
			current = self.head
			while current.next != None and current.value != before:
				current = current.next
			if current.value == before:
				node.next = current
				node.prev = current.prev
				current.prev.next = node
				current.prev = node
			else:
				print "Can't find that node; can't insert before."
		return self




# list = DoublyLinkedList()
# list.addNode(1).addNode(2).addNode(3).addNode(4).addNode(5).addNode(6).printAll()
# list.insertNode(10, 7).printAll()