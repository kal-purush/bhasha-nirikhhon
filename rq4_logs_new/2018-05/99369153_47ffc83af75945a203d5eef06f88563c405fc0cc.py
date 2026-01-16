#Simple single link list

class Node(object):
    def __init__(self,data):
        self.data = data
        self.next = None


class LinkList(object):
    def __init__(self):
        self.link = None
    
    def add(self,data):
        
        node = Node(data)
        node.next = self.link
        self.link = node
        
    def __str__(self):
        _link = self.link
        val = ""
        while _link:
            val += str(_link.data) + " --> "
            _link = _link.next
        return val[:-5]
     
    def get_last(self):
        return self.link.data

a=LinkList()
a.add(5)
a.add(3)
a.add(6)
a.add(3)
a.add(1)
a.add(10)
a.add(57)
a.add(3)
a.add(8)
a.add(5)
a.add(3)
a.add(100)
print(a)
print(a.get_last())