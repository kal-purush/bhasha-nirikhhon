# DESCRIPTION
# Design and implement a data structure for Least Recently Used (LRU) cache.
# It should support the following operations: get and put.

# get(key) - Get the value (will always be positive) of the key
# if the key exists in the cache, otherwise return -1.
# put(key, value) - Set or insert the value if the key is not already present.
# When the cache reached its capacity, it should invalidate the
# least recently used item before inserting a new item.

# The cache is initialized with a positive capacity.


# EXAMPLE:
# LRUCache cache = new LRUCache( 2 /* capacity */ );
# cache.put(1, 1);
# cache.put(2, 2);
# cache.get(1);       // returns 1
# cache.put(3, 3);    // evicts key 2
# cache.get(2);       // returns -1 (not found)
# cache.put(4, 4);    // evicts key 1
# cache.get(1);       // returns -1 (not found)
# cache.get(3);       // returns 3
# cache.get(4);       // returns 4

class LRUCache:

    def __init__(self, capacity: int):
        # hashtable of a doubly linked list
        # hashtable for the access time O(1)
        # specifically doubly linked list because
        # it give us O(1) add,remove to the head and tail
        self.capacity = capacity
        self.my_dict = {}
        self.head = Node(0, 0)
        self.tail = Node(0, 0)
        self.head.next = self.tail
        self.tail.prev = self.head

    def get(self, key: int) -> int:
        '''
        Time: O(1)
        Space: O(N)
        '''
        # search the hashtable for the key
        if key in self.my_dict:
            n = self.my_dict[key]
            # the node has been used remove it from the ll
            # add it to the tail
            self._remove(n)
            self._add(n)
            return n.value

        return -1

    def put(self, key: int, value: int) -> None:
        '''
        Time: O(1)
        Space: O(N)
        '''

        if key in self.my_dict:
            self._remove(self.my_dict[key])

        n = Node(key, value)
        self._add(n)
        self.my_dict[key] = n
        # if we have reached capacity remove the lru node
        if len(self.my_dict) > self.capacity:
            # make sure to not lose the reference to the ll
            n = self.head.next
            # then remove node
            self._remove(n)
            # deleting from the hashtable
            del self.my_dict[n.key]

    def _remove(self, node):
        # take the node that is closer to the head
        prev_node = node.prev
        # take the node afterward also
        next_node = node.next
        # set the prev node to circumvent the curr node
        # to the next node
        prev_node.next = next_node
        # set the next node to circumvent the curr node
        # to the prev node
        next_node.prev = prev_node

    def _add(self, node):
        '''adds to the end of the list'''
        # takes the second to last node reference
        prev_node = self.tail.prev
        #
        prev_node.next = node
        self.tail.prev = node
        node.prev = prev_node
        node.next = self.tail


class Node:

    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.prev = None
        self.next = None


# Your LRUCache object will be instantiated and called as such:
# obj = LRUCache(capacity)
# param_1 = obj.get(key)
# obj.put(key,value)