package com.coding.challenge.hash.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class FindTheLongestSubstringContainingVowelsInEvenCounts {
	private static final Map<Character, Integer> vowelOffset = Map.of('a', 0, 'e', 1, 'i', 2, 'o', 3, 'u', 4);

	FindTheLongestSubstringContainingVowelsInEvenCounts() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		assert findTheLongestSubstring("eleetminicoworoep") == 13;
		assert findTheLongestSubstring("leetcodeisgreat") == 5;
		assert findTheLongestSubstring("bcbcbc") == 6;
	}

	static int findTheLongestSubstring(String s) {
		final int n = s.length();
		final Map<List<Boolean>, Integer> oddEvenState = new HashMap<>();
		// sentinel value.
		oddEvenState.put(Arrays.asList(true, true, true, true, true), -1);
		final int[] vowelsCnt = new int[5];
		int l = 0;

		for (int i = 0; i < n; i++) {
			final char ch = s.charAt(i);
			if (vowelOffset.containsKey(ch)) {
				final int offset = vowelOffset.get(ch);
				vowelsCnt[offset] = vowelsCnt[offset] + 1;
			}
			final List<Boolean> currState = new ArrayList<>(5);
			for (int j = 0; j < 5; j++)
				currState.add(vowelsCnt[j] % 2 == 0);

			// check whether it is a valid state.
			if (oddEvenState.containsKey(currState))
				l = Math.max(l, i - oddEvenState.get(currState));
			else
				oddEvenState.put(currState, i);
		}
		return l;
	}
}