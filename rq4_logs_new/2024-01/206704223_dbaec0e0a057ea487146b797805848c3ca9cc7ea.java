package com.coding.challenge.dfs;

import java.util.ArrayList;
import java.util.List;

import com.coding.challenge.bst.TreeNode;

class PathSumII {
	private List<List<Integer>> l = new ArrayList<>();

	PathSumII() {

	}

	public static void main(String[] args) {
		final TreeNode root1 = new TreeNode(5,
				new TreeNode(4, new TreeNode(11, new TreeNode(7), new TreeNode(2)), null),
				new TreeNode(8, new TreeNode(13), new TreeNode(4, new TreeNode(5), new TreeNode(1))));
		final PathSumII ps1 = new PathSumII();
		ps1.visitTree(root1, 22, new ArrayList<>());
		System.out.println(ps1.l);

		final TreeNode root2 = new TreeNode(1, new TreeNode(2), new TreeNode(3));
		final PathSumII ps2 = new PathSumII();
		ps2.visitTree(root2, 5, new ArrayList<>());
		System.out.println(ps2.l);
	}

	public List<List<Integer>> pathSum(TreeNode root, int targetSum) {
		visitTree(root, targetSum, new ArrayList<>());
		return l;
	}

	private void visitTree(TreeNode node, int targetSum, List<Integer> pathVals) {
		if (node == null)
			return;
		pathVals.add(node.val);
		if (node.left == null && node.right == null && targetSum - node.val == 0)
			l.add(new ArrayList<>(pathVals));

		visitTree(node.left, targetSum - node.val, pathVals);
		visitTree(node.right, targetSum - node.val, pathVals);
		pathVals.remove(pathVals.size() - 1);
	}
}