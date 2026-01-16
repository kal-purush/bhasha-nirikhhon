package com.coding.challenge.two.pointer;

class ValidMountainArray {
	ValidMountainArray() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] a1 = { 2, 1 };
		assert !validMountainArray(a1);

		final int[] a2 = { 3, 5, 5 };
		assert !validMountainArray(a2);

		final int[] a3 = { 0, 3, 2, 1 };
		assert validMountainArray(a3);

		final int[] a4 = { 0, 2, 3, 4, 5, 2, 1, 0 };
		assert validMountainArray(a4);

		final int[] a5 = { 0, 2, 3, 3, 5, 2, 1, 0 };
		assert !validMountainArray(a5);
	}

	static boolean validMountainArray(int[] arr) {
		final int n = arr.length;
		int i = 0;
		while (i < n - 1 && arr[i + 1] > arr[i])
			i = i + 1;

		int j = n - 1;
		while (j > 0 && arr[j - 1] > arr[j])
			j = j - 1;

		return i > 0 && j < n - 1 && i == j;
	}
}