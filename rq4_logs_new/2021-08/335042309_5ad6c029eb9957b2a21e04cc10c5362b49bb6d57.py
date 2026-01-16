class Node:
    def __init__(self, key, val):
        self.key = key 
        self.val = val 
        self.next = None
        self.prev = None 

class LRUCache:
    def __init__(self, n):
        self.head = Node(0, 0)
        self.tail = Node(0, 0)
        self.hash = {}
        self.cap = n 
        self.count = 0 
        self.head.next = self.tail 
        self.tail.prev = self.head 

    def delete_node(self, node):
        node.prev.next = node.next 
        node.next.prev = node.prev

    def add_node(self, node):
        node.next = self.head.next 
        node.prev = self.head
        self.head.next.prev = node
        self.head.next = node 

    def move_to_head(self, node):
        self.delete_node(node)
        self.add_node(node)
    
    def delete_tail(self):
        node = self.tail.prev
        self.delete_node(node)
        return node

    def get(self, key):
        if key not in self.hash:
            return -1 
        else:
            node = self.hash[key]
            self.move_to_head(node)
            return node.val
    
    def put(self, key, val):
        if key in self.hash: # We need to update value of key
            node = self.hash[key]
            node.val = val 
            self.move_to_head(node)
        else: # key not present, need to check count and evict if necessary
            node = Node(key, val)
            self.hash[key] = node 
            self.add_node(node)
            self.count += 1
            if self.count>self.cap: # Cache full
                tail = self.delete_tail()
                del self.hash[tail.key]
                self.count -= 1


lru = LRUCache(2)
lru.put(1, 1)
lru.put(2,2)
lru.get(1)
lru.put(3,3)
lru.get(2)
lru.put(4,4)
lru.get(1)
lru.get(3)
lru.get(4)