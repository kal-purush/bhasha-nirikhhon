import itertools
class TrieNode:
    def __init__(self):
        self.children = {}
        self.end_of_word = False

class Trie:
    def __init__(self):
        self.root = TrieNode()

    def insert(self, word):
        node = self.root
        for char in word:
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        node.end_of_word = True

    def search(self, node, word):
        for char in word:
            if char not in node.children:
                return False
            node = node.children[char]
        return node.end_of_word

def build_trie(dictionary_file):
    trie = Trie()
    with open(dictionary_file, 'r') as f:
        for word in f.readlines():
            trie.insert(word.strip())
    return trie

def octordle_solver(trie, letters):
    results = []
    for word in itertools.permutations(letters, 3):  # Generate permutations of length 3
        if trie.search(trie.root, ''.join(word)):
            results.append(''.join(word))
    return results


# Load your dictionary file here
trie = build_trie('dictionary.txt')

# Provide the Octordle letters here
letters = ['j', 'a', 'g', 'k', 'd', 'o', 'i', 'd']


# Get all possible words
possible_words = octordle_solver(trie, letters)

print(possible_words)