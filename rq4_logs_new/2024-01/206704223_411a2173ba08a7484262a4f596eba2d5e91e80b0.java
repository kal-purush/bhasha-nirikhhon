package com.coding.challenge.dfs;

import java.util.ArrayList;
import java.util.List;

import com.coding.challenge.bst.TreeNode;

class BinaryTreePaths {
	final List<String> paths = new ArrayList<>();

	BinaryTreePaths() {

	}

	public static void main(String[] args) {
		final TreeNode root1 = new TreeNode(1, new TreeNode(2000, null, new TreeNode(5)), new TreeNode(3));
		final BinaryTreePaths p1 = new BinaryTreePaths();
		p1.binaryTreePaths(root1);
		System.out.println(p1.paths);

		final BinaryTreePaths p2 = new BinaryTreePaths();
		p2.binaryTreePaths(new TreeNode(1));
		System.out.println(p2.paths);
	}

	List<String> binaryTreePaths(TreeNode root) {
		visitTree(root, new StringBuilder());
		return paths;
	}

	private void visitTree(TreeNode root, StringBuilder currPath) {
		if (root == null)
			return;
		currPath.append(root.val);
		if (root.left == null && root.right == null)
			paths.add(currPath.toString());
		currPath.append("->");
		visitTree(root.left, currPath);
		visitTree(root.right, currPath);
		final int charsToBeDeleted = String.valueOf(root.val).length() + 2;
		// at most 7 chars would require deleting.
		for (int i = 0; i < charsToBeDeleted; i++)
			currPath.deleteCharAt(currPath.length() - 1);
	}
}