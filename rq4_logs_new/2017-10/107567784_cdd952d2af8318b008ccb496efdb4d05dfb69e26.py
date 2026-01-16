class Node:
    def __init__(self,initdata): #creates node with data given
        self.data = initdata
        self.next = None

    def getdata(self): #returns currents node's data
        return self.data

    def getnext(self): #gets the next node in the chain
        return self.next

    def setdata(self, newdata):#changes the data in the node
        self.data = newdata

    def setnext(self,newnext): #changes the next node in the list
        self.next = newnext

class Unorderedlist:
    def __init__(self):
        self.head = None #creates a new unorderedlist

    def add(self, item): #adds a node to teh list
        node = Node(item)
        node.setnext(self.head)
        self.head = node #changes the head of the list to the current node

    def isempty(self):
        return self.head == None

    def size(self): #iterates through list calling each next node and returning a total
        current = self.head
        count = 0
        while current != None:
            count = count + 1
            current = current.getnext()
        return count

    def printlist(self): #similar to size but adds values to a regular list
        current = self.head
        nodelist = []
        while current != None:
            currentnode = current.getdata()
            nodelist.append(currentnode)
            current = current.getnext()
        return nodelist

    def search(self,searchitem): #iterates through list of nodes until search value found
        current = self.head
        found = False
        while current != None and not found:
            currentnode = current.getdata()
            if currentnode != searchitem:
                current = current.getnext()
            else:
                found = True
        return found

    def delete(self,searchitem): #finds node in the same manor as search, also finds the nodes before and after and attaches them, removing chosen node from the list
        current = self.head #things that need changing, if the node searched is the final node in the list will throw error, the deleted node still exists in memory
        found = False
        prevnode = None
        while current != None and not found:
            currentnode = current.getdata()
            nextnode = current.getnext()
            nextnodedata = nextnode.getdata()
            if nextnodedata == searchitem:
                prevnode = current
            if currentnode == searchitem:
                found = True
                if prevnode == None:
                    self.head = current.getnext()
                else:
                    frontnode = current.getnext()
                    prevnode.setnext(frontnode)
            else:
                current = current.getnext()
        return found
        print (found)
        print (nextnodedata)





newlist = Unorderedlist()

newlist.add(19)
newlist.add(18)
newlist.add(17)
newlist.add(16)
newlist.add(15)

print(newlist.size())
print(newlist.printlist())
print(newlist.search(16))
newlist.delete(19)
print(newlist.printlist())