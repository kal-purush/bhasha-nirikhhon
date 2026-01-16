package com.coding.challenge.dp;

import com.coding.challenge.BinarySearch;

class MinimumRemovalsToMakeMountainArray {
	MinimumRemovalsToMakeMountainArray() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] nums1 = { 1, 3, 1 };
		assert minimumMountainRemovals(nums1) == 0;

		final int[] nums2 = { 2, 1, 1, 5, 6, 2, 3, 1 };
		assert minimumMountainRemovals(nums2) == 3;
	}

	static int minimumMountainRemovals(int[] nums) {
		final int[] lisLen = lisLength(nums);
		final int n = nums.length;
		final int[] reversedNums = new int[n];
		System.arraycopy(nums, 0, reversedNums, 0, n);
		reverse(reversedNums);
		final int[] ldsLen = lisLength(reversedNums);
		int l = 0;
		for (int i = 0; i < n; i++)
			if (lisLen[i] > 1 && ldsLen[n - 1 - i] > 1)
				l = Math.max(l, lisLen[i] + ldsLen[n - 1 - i] - 1);

		return n - l;
	}

	/**
	 * Reverses the input array in place.
	 * 
	 * @param a sequence of integers.
	 */
	private static void reverse(int[] a) {
		final int n = a.length;
		for (int i = 0; i < n / 2; i++) {
			final int tmp = a[i];
			a[i] = a[n - i - 1];
			a[n - i - 1] = tmp;
		}
	}

	/**
	 * Computes the LIS length when nums[i] is chosen as the last element in LIS.
	 * 
	 * @param nums input integer sequence.
	 * @return An array of length similar to the size of the input sequence where
	 *         each element represents the length of the LIS when the ith element is
	 *         included in LIS as the last element.
	 */
	private static int[] lisLength(int[] nums) {
		final int n = nums.length;
		final int[] l = new int[n];
		// always maintaining sorted order of the array a.
		final int[] a = new int[n];
		a[0] = nums[0];
		l[0] = 1;
		for (int i = 1, size = 1; i < n; i++) {
			// find nums[i]'s successor in array a and replace it with ith element.
			/*
			 * Note that we use (nums[i] - 1) expression to handle duplicate elements by
			 * replacing them without inserting.
			 */
			final int j = BinarySearch.successor(a, nums[i] - 1, 0, size - 1);
			// add a new element to the end of the sorted array a.
			if (j >= size)
				size = size + 1;
			a[j] = nums[i];
			l[i] = j + 1;
		}
		return l;
	}
}