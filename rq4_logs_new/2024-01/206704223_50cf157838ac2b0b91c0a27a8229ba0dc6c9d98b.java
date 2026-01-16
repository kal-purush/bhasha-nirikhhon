package com.coding.challenge.dfs;

import com.coding.challenge.bst.TreeNode;

class LongestUnivaluePath {
	LongestUnivaluePath() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final TreeNode root1 = new TreeNode(5, new TreeNode(4, new TreeNode(1), new TreeNode(1)),
				new TreeNode(5, null, new TreeNode(5)));
		assert longestUnivaluePath(root1) == 2;

		final TreeNode root2 = new TreeNode(1, new TreeNode(4, new TreeNode(4), new TreeNode(4)),
				new TreeNode(5, null, new TreeNode(5)));
		assert longestUnivaluePath(root2) == 2;

		assert longestUnivaluePath(null) == 0;

		final TreeNode root3 = new TreeNode(1, null, new TreeNode(1, new TreeNode(1, new TreeNode(1), new TreeNode(1)),
				new TreeNode(1, new TreeNode(1), null)));
		assert longestUnivaluePath(root3) == 4;
	}

	static int longestUnivaluePath(TreeNode root) {
		final int[] a = new int[1];
		visitTree(root, a);
		return a[0];
	}

	private static int visitTree(TreeNode node, int[] a) {
		if (node == null)
			return 0;

		int uniNodeCount = 1;
		int maxSideLength = 0;
		final int l1 = visitTree(node.left, a);
		if (node.left != null && node.left.val == node.val) {
			uniNodeCount = uniNodeCount + l1;
			maxSideLength = Math.max(maxSideLength, l1);
		}
		final int l2 = visitTree(node.right, a);
		if (node.right != null && node.right.val == node.val) {
			uniNodeCount = uniNodeCount + l2;
			maxSideLength = Math.max(maxSideLength, l2);
		}
		a[0] = Math.max(a[0], uniNodeCount - 1);
		return maxSideLength + 1;
	}
}