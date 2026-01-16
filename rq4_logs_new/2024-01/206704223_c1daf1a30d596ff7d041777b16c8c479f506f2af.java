package com.coding.challenge.hash.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

class RandomPickIndex {
	private final Map<Integer, List<Integer>> t;

	RandomPickIndex(int[] nums) {
		t = new HashMap<>();
		final int n = nums.length;
		for (int i = 0; i < n; i++)
			t.computeIfAbsent(nums[i], unused -> new ArrayList<>()).add(i);
	}

	public static void main(String[] args) {
		final int[] nums1 = { 1, 2, 3, 3, 3 };
		final RandomPickIndex randPickIdx = new RandomPickIndex(nums1);
		System.out.println(randPickIdx.pick(3));
		System.out.println(randPickIdx.pick(1));
		System.out.println(randPickIdx.pick(3));
	}

	public int pick(int target) {
		final List<Integer> indices = t.get(target);
		final int randIdx = ThreadLocalRandom.current().nextInt(indices.size());
		return indices.get(randIdx);
	}
}