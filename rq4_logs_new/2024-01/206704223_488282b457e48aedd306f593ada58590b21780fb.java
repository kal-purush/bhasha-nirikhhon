package com.coding.challenge.trie;

import java.util.Arrays;

class SumOfPrefixScoresOfStrings {
	private static final char FIRST_LETTER = 'a';

	SumOfPrefixScoresOfStrings() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final String[] words1 = { "abc", "ab", "bc", "b" };
		final int[] prefixScore1 = { 5, 4, 3, 2 };
		assert Arrays.equals(prefixScore1, sumPrefixScores(words1));

		final String[] words2 = { "abcd" };
		final int[] prefixScore2 = { 4 };
		assert Arrays.equals(prefixScore2, sumPrefixScores(words2));
	}

	static int[] sumPrefixScores(String[] words) {
		final PrefixScoreTrie prefixTree = new PrefixScoreTrie();
		for (String w : words)
			prefixTree.addWord(w);

		final int n = words.length;
		final int[] a = new int[n];
		for (int i = 0; i < n; i++)
			a[i] = prefixTree.findScore(words[i]);

		return a;
	}

	static class PrefixScoreTrie {
		private final TrieNode root = new TrieNode();

		void addWord(String word) {
			addWord(word, 0, root);
		}

		private void addWord(String word, int i, TrieNode parent) {
			if (i == word.length())
				return;
			if (parent.children[word.charAt(i) - FIRST_LETTER] == null)
				parent.children[word.charAt(i) - FIRST_LETTER] = new TrieNode();
			final TrieNode currNode = parent.children[word.charAt(i) - FIRST_LETTER];
			currNode.wordCnt = currNode.wordCnt + 1;
			addWord(word, i + 1, currNode);
		}

		int findScore(String word) {
			return findScore(word, 0, root);
		}

		private int findScore(String word, int i, TrieNode parent) {
			// trivial case of the recursion.
			if (parent == null)
				return 0;
			if (i == word.length())
				return parent.wordCnt;

			return findScore(word, i + 1, parent.children[word.charAt(i) - FIRST_LETTER]) + parent.wordCnt;
		}
	}

	static class TrieNode {
		private final TrieNode[] children = new TrieNode[26];
		private int wordCnt = 0;
	}
}