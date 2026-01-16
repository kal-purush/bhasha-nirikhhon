package com.coding.challenge.dfs;

import java.util.HashMap;

import com.coding.challenge.bst.TreeNode;

class PathSumIII {
	PathSumIII() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final TreeNode root1 = new TreeNode(10, new TreeNode(5, new TreeNode(3, new TreeNode(3), new TreeNode(-2)),
				new TreeNode(2, null, new TreeNode(1))), new TreeNode(-3, null, new TreeNode(11)));
		assert pathSum(root1, 8) == 3;

		assert pathSum(new TreeNode(1), 0) == 0;
	}

	static int pathSum(TreeNode root, int targetSum) {
		final HashMap<Long, Integer> f = new HashMap<>();
		f.put(0l, 1);
		return visitTree(root, targetSum, 0, f);
	}

	private static int visitTree(TreeNode root, int targetSum, long pathSum, HashMap<Long, Integer> f) {
		if (root == null)
			return 0;
		final long currSum = pathSum + root.val;
		f.merge(currSum, 1, Integer::sum);
		final int leftPaths = visitTree(root.left, targetSum, currSum, f);
		final int rightPaths = visitTree(root.right, targetSum, currSum, f);
		// unchoose action.
		if (f.get(currSum) == 1)
			f.remove(currSum);
		else
			f.put(currSum, f.get(currSum) - 1);
		return f.getOrDefault(currSum - targetSum, 0) + leftPaths + rightPaths;
	}
}