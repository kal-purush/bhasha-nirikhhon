class TrieNode:
    
    def __init__(self):

        self.children = [None]*26
        self.isEndOfWord = False 

class Trie:

    def __init__(self):
        self.root = TrieNode()

    def _charToInd(self, ch):
        return ord(ch)-ord('a')

    def _getNode(self): # Get a new node which is not root
        return TrieNode()

    def insert(self, key):
        curr = self.root
        length = len(key)

        for i in range(length):
            idx = self._charToInd(key[i])

            if not curr.children[idx]:
                curr.children[idx] = self._getNode()
            
            curr = curr.children[idx]
        
        curr.isEndOfWord = True 

    def search(self, key):

        curr = self.root
        length = len(key)

        for i in range(length):
            idx = self._charToInd(key[i])

            if not curr.children[idx]:
                return False 
            curr = curr.children[idx]

        return curr.isEndOfWord

    def startsWith(self, pfx):

        curr = self.root 
        length = len(pfx)

        for i in range(length):
            idx = self._charToInd(pfx[i])

            if not curr.children[idx]:
                return False 
            
            curr = curr.children[idx]
        
        return True 