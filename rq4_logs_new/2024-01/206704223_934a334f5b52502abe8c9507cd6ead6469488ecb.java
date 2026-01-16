package com.coding.challenge.dfs;

import java.util.ArrayList;
import java.util.List;

import com.coding.challenge.bst.TreeNode;

class EvenOddTree {
	EvenOddTree() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final TreeNode root1 = new TreeNode(1,
				new TreeNode(10, new TreeNode(3, new TreeNode(12), new TreeNode(8)), null),
				new TreeNode(4, new TreeNode(7, new TreeNode(6), null), new TreeNode(9, null, new TreeNode(2))));
		assert isEvenOddTree(root1);

		final TreeNode root2 = new TreeNode(5, new TreeNode(4, new TreeNode(3), new TreeNode(3)),
				new TreeNode(2, new TreeNode(7), null));
		assert !isEvenOddTree(root2);

		final TreeNode root3 = new TreeNode(5, new TreeNode(9, new TreeNode(3), new TreeNode(5)),
				new TreeNode(1, new TreeNode(7), null));
		assert !isEvenOddTree(root3);

		assert !isEvenOddTree(new TreeNode(2));

		final TreeNode root4 = new TreeNode(11,
				new TreeNode(18, new TreeNode(3), new TreeNode(7, null, new TreeNode(18, null, new TreeNode(6)))),
				new TreeNode(14));
		assert !isEvenOddTree(root4);
	}

	static boolean isEvenOddTree(TreeNode root) {
		return visitTree(root, new ArrayList<>(), 0);
	}

	private static boolean visitTree(TreeNode node, List<Integer> levels, int level) {
		if (node == null)
			return true;

		// there are previous elements in this level.
		if (levels.size() > level) {
			if (level % 2 == 0 && levels.get(level) >= node.val)
				return false;
			if (level % 2 == 1 && levels.get(level) <= node.val)
				return false;
		}
		if (level % 2 == 0 && node.val % 2 == 0)
			return false;
		if (level % 2 == 1 && node.val % 2 == 1)
			return false;

		if (levels.size() == level)
			levels.add(node.val);
		else
			levels.set(level, node.val);

		final boolean left = visitTree(node.left, levels, level + 1);
		if (!left)
			return false;
		return visitTree(node.right, levels, level + 1);
	}
}