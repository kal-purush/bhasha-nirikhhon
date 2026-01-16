#Time complexity :                      Space complexity:
    #insert : O(L)                              insert: O(L)
    #search : O(L)                              search : O(1)
    #prefix : O(p)                              prefix : O(1)
#Did code run successfully on Leetcode : yes


class TrieNode:
    def __init__(self,end=None,children=None):
        self.isend= False
        self.children = [False]*26
        
class Trie:

    def __init__(self):
        """
        Initialize your data structure here.
        """
        self.root  = TrieNode()

    def insert(self, word: str) -> None:
        """
        Inserts a word into the trie.
        """
        cur = self.root
        for index,w in enumerate(word):
            word_index = ord(w)-ord('a')
            if not cur.children[word_index]:
                cur.children[word_index] = TrieNode()
            cur = cur.children[word_index]
        cur.isend = True
        

    def search(self, word: str) -> bool:
        """
        Returns if the word is in the trie.
        """
        cur = self.root
        for index,w in enumerate(word):
            word_index = ord(w)-ord('a')
            if not cur.children[word_index]:
                return False
            cur = cur.children[word_index]
        return cur.isend

    def startsWith(self, prefix: str) -> bool:
        """
        Returns if there is any word in the trie that starts with the given prefix.
        """
        cur = self.root
        for index,w in enumerate(prefix):
            word_index = ord(w)-ord('a')
            if not cur.children[word_index]:
                return False
            cur = cur.children[word_index]
        
        return True


# Your Trie object will be instantiated and called as such:
# obj = Trie()
# obj.insert(word)
# param_2 = obj.search(word)
# param_3 = obj.startsWith(prefix)