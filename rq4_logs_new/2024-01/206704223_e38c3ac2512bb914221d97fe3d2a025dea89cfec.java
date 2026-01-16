package com.coding.challenge.trie;

class WordDictionary {
	private static final char FIRST_LETTER = 'a';
	private final TrieNode root = new TrieNode();

	public WordDictionary() {

	}

	public static void main(String[] args) {
		final WordDictionary wordDictionary = new WordDictionary();
		wordDictionary.addWord("bad");
		wordDictionary.addWord("dad");
		wordDictionary.addWord("mad");
		assert !wordDictionary.search("pad");
		assert wordDictionary.search("bad");
		assert wordDictionary.search(".ad");
		assert wordDictionary.search("b..");

		final WordDictionary wordDictionary2 = new WordDictionary();
		wordDictionary2.addWord("a");
		wordDictionary2.addWord("a");
		assert wordDictionary2.search(".");
		assert wordDictionary2.search("a");
		assert !wordDictionary2.search("aa");
		assert wordDictionary2.search("a");
		assert !wordDictionary2.search(".a");
		assert !wordDictionary2.search("a.");

		final WordDictionary wordDictionary3 = new WordDictionary();
		wordDictionary3.addWord("at");
		wordDictionary3.addWord("and");
		wordDictionary3.addWord("an");
		wordDictionary3.addWord("add");
		assert !wordDictionary3.search("a");
		assert !wordDictionary3.search(".at");
		wordDictionary3.addWord("bat");
		assert wordDictionary3.search(".at");
		assert wordDictionary3.search("an.");
		assert !wordDictionary3.search("a.d.");
		assert !wordDictionary3.search("b.");
		assert wordDictionary3.search("a.d");
		assert !wordDictionary3.search(".");
	}

	public void addWord(String word) {
		addWord(word, 0, root);
	}

	private void addWord(String word, int i, TrieNode parent) {
		if (i == word.length()) {
			parent.word = true;
			return;
		}
		if (parent.children[word.charAt(i) - FIRST_LETTER] == null)
			parent.children[word.charAt(i) - FIRST_LETTER] = new TrieNode();
		addWord(word, i + 1, parent.children[word.charAt(i) - FIRST_LETTER]);
	}

	public boolean search(String word) {
		return search(word, 0, root);
	}

	boolean search(String word, int i, TrieNode parent) {
		// trivial case of the recursion.
		if (parent == null)
			return false;
		if (i == word.length())
			return parent.word;

		if (word.charAt(i) != '.')
			return search(word, i + 1, parent.children[word.charAt(i) - FIRST_LETTER]);
		// handle the wildcard here.
		for (TrieNode child : parent.children)
			if (search(word, i + 1, child))
				return true;

		return false;
	}

	static class TrieNode {
		private final TrieNode[] children = new TrieNode[26];
		private boolean word;
	}
}