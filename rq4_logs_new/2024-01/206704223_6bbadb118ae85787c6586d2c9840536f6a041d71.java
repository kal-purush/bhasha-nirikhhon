package com.coding.challenge.dfs;

import com.coding.challenge.bst.TreeNode;

class LowestCommonAncestorOfDeepestLeaves {
	LowestCommonAncestorOfDeepestLeaves() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final TreeNode root = new TreeNode(3,
				new TreeNode(5, new TreeNode(6), new TreeNode(2, new TreeNode(7), new TreeNode(4))),
				new TreeNode(1, new TreeNode(0), new TreeNode(8)));
		System.out.println(lcaDeepestLeaves(root));
	}

	static TreeNode lcaDeepestLeaves(TreeNode root) {
		return visitTree(root).node;
	}

	// One-pass solution.
	private static TreeNodeDepth visitTree(TreeNode node) {
		if (node == null)
			return new TreeNodeDepth(node, 0);

		final TreeNodeDepth leftDepth = visitTree(node.left);
		final TreeNodeDepth rightDepth = visitTree(node.right);
		if (leftDepth.depth == rightDepth.depth)
			return new TreeNodeDepth(node, leftDepth.depth + 1);
		if (leftDepth.depth > rightDepth.depth) {
			leftDepth.depth = leftDepth.depth + 1;
			return leftDepth;
		}

		rightDepth.depth = rightDepth.depth + 1;
		return rightDepth;
	}

	static class TreeNodeDepth {
		private final TreeNode node;
		private int depth;

		TreeNodeDepth(TreeNode node, int depth) {
			this.node = node;
			this.depth = depth;
		}
	}
}