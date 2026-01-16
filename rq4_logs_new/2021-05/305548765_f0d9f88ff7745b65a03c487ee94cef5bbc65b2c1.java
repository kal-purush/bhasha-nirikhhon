package com.linearsearch;

public class LinearSearch {
	private LinearSearch() {}

	static int search(int[] data, int target) {
		for (int i = 0; i < data.length; i++) {
			if (data[i] == target) {
				return i;
			}
		}
		return -1;
	}

	public static void main(String[] args) {
		int[] data = {1, 2, 4, 5, 6};
		System.out.println(search(data, 5));
	}
}