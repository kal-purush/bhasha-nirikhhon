package com.coding.challenge.dfs;

import com.coding.challenge.bst.TreeNode;

class SmallestSubtreeWithAllTheDeepestNodes {
	SmallestSubtreeWithAllTheDeepestNodes() {

	}

	public static void main(String[] args) {
		final TreeNode root = new TreeNode(3,
				new TreeNode(5, new TreeNode(6), new TreeNode(2, new TreeNode(7), new TreeNode(4))),
				new TreeNode(1, new TreeNode(0), new TreeNode(8)));
		System.out.println(subtreeWithAllDeepest(root));
	}

	static TreeNode subtreeWithAllDeepest(TreeNode root) {
		final int depth = findDepth(root);
		return visitTree(root, depth, 0);
	}

	private static int findDepth(TreeNode node) {
		if (node == null)
			return -1;

		final int dl = findDepth(node.left);
		final int dr = findDepth(node.right);
		return Math.max(dl, dr) + 1;
	}

	private static TreeNode visitTree(TreeNode node, int depth, int currDepth) {
		if (node == null)
			return null;
		if (currDepth == depth)
			return node;

		final TreeNode leftLca = visitTree(node.left, depth, currDepth + 1);
		final TreeNode rightLca = visitTree(node.right, depth, currDepth + 1);
		if (leftLca != null && rightLca != null)
			return node;
		if (leftLca != null)
			return leftLca;
		return rightLca;
	}
}