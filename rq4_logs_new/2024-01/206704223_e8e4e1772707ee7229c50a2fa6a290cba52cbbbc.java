package com.coding.challenge.backtracking;

import java.util.Set;

class FindUniqueBinaryString {
	FindUniqueBinaryString() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final String[] nums1 = { "01", "10" };
		System.out.println(findDifferentBinaryString(nums1));

		final String[] nums2 = { "00", "01" };
		System.out.println(findDifferentBinaryString(nums2));

		final String[] nums3 = { "111", "011", "001" };
		System.out.println(findDifferentBinaryString(nums3));
	}

	static String findDifferentBinaryString(String[] nums) {
		return backtrack(Set.of(nums), nums.length, "");
	}

	private static String backtrack(Set<String> nums, int n, String binStr) {
		// real backtracking problem instead of an exhaustive search.
		// trivial case of the recursion.
		if (n == 0) {
			if (nums.contains(binStr))
				return "";
			else
				return binStr;
		}

		// choose 0 and explore
		final String candidate = backtrack(nums, n - 1, binStr + '0');
		if (!candidate.isEmpty())
			return candidate;
		// choose 1 and explore
		return backtrack(nums, n - 1, binStr + '1');
	}
}