package com.coding.challenge.dp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class WordBreak {
	WordBreak() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		assert wordBreak("leetcode", Arrays.asList("leet", "code"));
		assert wordBreak("applepenapple", Arrays.asList("apple", "pen"));
		assert !wordBreak("catsandog", Arrays.asList("cats", "dog", "sand", "and", "cat"));
	}

	static boolean wordBreak(String s, List<String> wordDict) {
		final int n = s.length();
		final Set<String> dictionary = new HashSet<String>(wordDict);
		final boolean[] wb = new boolean[n];

		for (int j = 0; j < n; j++) {
			wb[j] = dictionary.contains(s.substring(0, j + 1));
			for (int i = 0; i < j && !wb[j]; i++)
				wb[j] = wb[i] && dictionary.contains(s.substring(i + 1, j + 1));
		}
		return wb[n - 1];
	}
}