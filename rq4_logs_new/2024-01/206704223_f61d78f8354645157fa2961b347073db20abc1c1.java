package com.coding.challenge.backtracking;

class MaximumScoreWordsFormedByLetters {
	private static final char FIRST_LETTER = 'a';

	MaximumScoreWordsFormedByLetters() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final String[] words1 = { "dog", "cat", "dad", "good" };
		final char[] letters1 = { 'a', 'a', 'c', 'd', 'd', 'd', 'g', 'o', 'o' };
		final int[] score1 = { 1, 0, 9, 5, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
		assert maxScoreWords(words1, letters1, score1) == 23;

		final String[] words2 = { "xxxz", "ax", "bx", "cx" };
		final char[] letters2 = { 'z', 'a', 'b', 'c', 'x', 'x', 'x' };
		final int[] score2 = { 4, 4, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 0, 10 };
		assert maxScoreWords(words2, letters2, score2) == 27;

		final String[] words3 = { "leetcode" };
		final char[] letters3 = { 'l', 'e', 't', 'c', 'o', 'd' };
		final int[] score3 = { 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0 };
		assert maxScoreWords(words3, letters3, score3) == 0;
	}

	static int maxScoreWords(String[] words, char[] letters, int[] score) {
		final int[] a = new int[1];
		final int[] letterFreq = new int[26];
		for (char ch : letters)
			letterFreq[ch - FIRST_LETTER] = letterFreq[ch - FIRST_LETTER] + 1;
		backtrack(words, letterFreq, score, 0, a, 0);
		return a[0];
	}

	private static void backtrack(String[] words, int[] letterFreq, int[] score, int i, int[] ans, int currScore) {
		// trivial case of the recursion.
		if (i == words.length)
			return;

		// exclude the word.
		backtrack(words, letterFreq, score, i + 1, ans, currScore);
		// include word.
		final String word = words[i];
		// choose.
		boolean canInclude = true;
		int wordScore = 0;
		for (char ch : word.toCharArray()) {
			wordScore = wordScore + score[ch - FIRST_LETTER];
			letterFreq[ch - FIRST_LETTER] = letterFreq[ch - FIRST_LETTER] - 1;
			canInclude = canInclude && letterFreq[ch - FIRST_LETTER] >= 0;
		}
		// explore.
		if (canInclude) {
			ans[0] = Math.max(ans[0], currScore + wordScore);
			backtrack(words, letterFreq, score, i + 1, ans, currScore + wordScore);
		}
		// unchoose.
		for (char ch : word.toCharArray())
			letterFreq[ch - FIRST_LETTER] = letterFreq[ch - FIRST_LETTER] + 1;
	}
}