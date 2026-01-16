package com.coding.challenge.dfs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.coding.challenge.bst.TreeNode;

class FindLargestValueInEachTreeRow {
	FindLargestValueInEachTreeRow() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final TreeNode root1 = new TreeNode(1, new TreeNode(3, new TreeNode(5), new TreeNode(3)),
				new TreeNode(2, null, new TreeNode(9)));
		assert Arrays.asList(1, 3, 9).equals(largestValues(root1));
	}

	static List<Integer> largestValues(TreeNode root) {
		final List<Integer> levelMax = new ArrayList<>();
		visitTree(root, levelMax, 0);
		return levelMax;
	}

	private static void visitTree(TreeNode node, List<Integer> levelMax, int level) {
		// trivial case of the recursion.
		if (node == null)
			return;
		if (levelMax.size() - 1 < level)
			levelMax.add(node.val);
		else if (levelMax.get(level) < node.val)
			levelMax.set(level, node.val);
		visitTree(node.left, levelMax, level + 1);
		visitTree(node.right, levelMax, level + 1);
	}
}