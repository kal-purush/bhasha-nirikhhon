package com.coding.challenge.hash.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class WordSubsets {
	private static final char FIRST_LETTER = 'a';

	WordSubsets() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final String[] words1 = { "amazon", "apple", "facebook", "google", "leetcode" };
		final String[] words2 = { "e", "o" };
		assert Arrays.asList("facebook", "google", "leetcode").equals(wordSubsets(words1, words2));

		final String[] words3 = { "amazon", "apple", "facebook", "google", "leetcode" };
		final String[] words4 = { "l", "e" };
		assert Arrays.asList("apple", "google", "leetcode").equals(wordSubsets(words3, words4));
	}

	static List<String> wordSubsets(String[] words1, String[] words2) {
		final int[] highestFreq = new int[26];
		for (String word : words2) {
			final int[] currFreq = new int[26];
			for (char ch : word.toCharArray())
				currFreq[ch - FIRST_LETTER] = currFreq[ch - FIRST_LETTER] + 1;

			for (int i = 0; i < 26; i++)
				highestFreq[i] = Math.max(highestFreq[i], currFreq[i]);
		}

		final List<String> universalStrs = new ArrayList<>();
		for (String word : words1) {
			final int[] letterFreq = new int[26];
			for (char ch : word.toCharArray())
				letterFreq[ch - FIRST_LETTER] = letterFreq[ch - FIRST_LETTER] + 1;
			boolean isUniversal = true;
			for (int i = 0; i < 26; i++)
				isUniversal = isUniversal && letterFreq[i] >= highestFreq[i];

			if (isUniversal)
				universalStrs.add(word);
		}
		return universalStrs;
	}
}