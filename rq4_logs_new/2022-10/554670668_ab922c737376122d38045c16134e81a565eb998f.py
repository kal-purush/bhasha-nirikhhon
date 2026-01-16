class Node:
  def __init__(self, data):
    self.data = data
    self.next = None

#class Linked List
class LinkedList:
  def __init__(self):
    self.head = None

  #Add new element at the end of the list
  def push_back(self, newElement):
    newNode = Node(newElement)
    if(self.head == None):
      self.head = newNode
      return
    else:
      temp = self.head
      while(temp.next != None):
        temp = temp.next
      temp.next = newNode

  #Delete an element at the given position
  def pop_at(self, position):     
    if(position < 1):
      print("\nposition should be >= 1.")
    elif (position == 1 and self.head != None):
      nodeToDelete = self.head
      self.head = self.head.next
      nodeToDelete = None
    else:    
      temp = self.head
      for i in range(1, position-1):
        if(temp != None):
          temp = temp.next   
      if(temp != None and temp.next != None):
        nodeToDelete = temp.next
        temp.next = temp.next.next
        nodeToDelete = None 
      else:
        print("\nThe node is already null.") 

  #display the content of the list
  def PrintList(self):
    temp = self.head
    if(temp != None):
      print("The list contains:", end=" ")
      while (temp != None):
        print(temp.data, end=" ")
        temp = temp.next
      print()
    else:
      print("The list is empty.")
    