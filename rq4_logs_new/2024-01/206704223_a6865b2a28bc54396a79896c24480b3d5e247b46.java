package com.coding.challenge.simulation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class BuildAnArrayWithStackOperations {
	BuildAnArrayWithStackOperations() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final int[] target1 = { 1, 3 };
		final List<String> ops1 = Arrays.asList("Push", "Push", "Pop", "Push");
		assert ops1.equals(buildArray(target1, 3));

		final int[] target2 = { 1, 2, 3 };
		final List<String> ops2 = Arrays.asList("Push", "Push", "Push");
		assert ops2.equals(buildArray(target2, 3));

		final int[] target3 = { 1, 2 };
		final List<String> ops3 = Arrays.asList("Push", "Push");
		assert ops3.equals(buildArray(target3, 4));
	}

	static List<String> buildArray(int[] target, int n) {
		final int l = target.length;
		final List<String> ops = new ArrayList<>();
		for (int i = 0, j = 1; i < l; i++) {
			while (j != target[i]) {
				ops.add("Push");
				ops.add("Pop");
				j = j + 1;
			}
			ops.add("Push");
			j = j + 1;
		}
		return ops;
	}
}