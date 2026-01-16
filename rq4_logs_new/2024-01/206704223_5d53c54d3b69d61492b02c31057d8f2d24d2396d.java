package com.coding.challenge.binary.search;

class FindInMountainArray {
	FindInMountainArray() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
//		final int[] nums1 = { 1, 2, 3, 4, 5, 3, 1 };
//		System.out.println(findInMountainArray(3, nums1));
//
//		final int[] nums2 = { 0, 1, 2, 4, 2, 1 };
//		System.out.println(findInMountainArray(3, nums2));
	}

	static int findInMountainArray(int target, MountainArray mountainArr) {
		final int peakIdx = findPeak(mountainArr);
		final int idx = search(mountainArr, target, 0, peakIdx);
		if (idx >= 0)
			return idx;

		return decreasingBinarySearch(mountainArr, target, peakIdx + 1, mountainArr.length() - 1);
	}

	private static int findPeak(MountainArray mountainArr) {
		final int n = mountainArr.length();
		int l = 0;
		int r = n - 1;
		while (l < r) {
			final int m = (l + r) / 2;
			if (mountainArr.get(m) < mountainArr.get(m + 1))
				l = m + 1;
			else
				r = m;
		}
		return l;
	}

	private static int search(MountainArray mountainArr, int target, int i, int j) {
		int l = i;
		int r = j;
		while (l <= r) {
			final int m = (l + r) / 2;
			if (mountainArr.get(m) == target)
				return m;
			if (mountainArr.get(m) > target)
				r = m - 1;
			else
				l = m + 1;
		}
		return -1;
	}

	private static int decreasingBinarySearch(MountainArray mountainArr, int target, int i, int j) {
		int l = i;
		int r = j;
		while (l <= r) {
			final int m = (l + r) / 2;
			if (mountainArr.get(m) == target)
				return m;
			if (mountainArr.get(m) > target)
				l = m + 1;
			else
				r = m - 1;
		}
		return -1;
	}

	static interface MountainArray {
		int get(int k);

		int length();
	}
}