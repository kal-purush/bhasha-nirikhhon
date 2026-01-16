package com.coding.challenge.string;

class CountTheNumberOfVowelStringsInRange {
	CountTheNumberOfVowelStringsInRange() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final String[] words = { "are", "amy", "u" };
		assert vowelStrings(words, 0, 2) == 2;

		final String[] words2 = { "hey", "aeo", "mu", "ooo", "artro" };
		assert vowelStrings(words2, 1, 4) == 3;
	}

	static int vowelStrings(String[] words, int left, int right) {
		int c = 0;
		for (int i = left; i <= right; i++) {
			final String word = words[i];
			final char firstLetter = word.charAt(0);
			if (firstLetter == 'a' || firstLetter == 'e' || firstLetter == 'i' || firstLetter == 'o'
					|| firstLetter == 'u') {
				final char lastLetter = word.charAt(word.length() - 1);
				if (lastLetter == 'a' || lastLetter == 'e' || lastLetter == 'i' || lastLetter == 'o'
						|| lastLetter == 'u')
					c = c + 1;
			}
		}
		return c;
	}
}