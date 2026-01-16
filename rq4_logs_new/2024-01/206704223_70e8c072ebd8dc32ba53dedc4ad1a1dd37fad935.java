package com.coding.challenge.dfs;

import com.coding.challenge.bst.TreeNode;

class ConvertBSTToGreaterTree {
	ConvertBSTToGreaterTree() {
		throw new AssertionError();
	}

	public static void main(String[] args) {
		final TreeNode root1 = new TreeNode(4, new TreeNode(1, new TreeNode(0), new TreeNode(2, null, new TreeNode(3))),
				new TreeNode(6, new TreeNode(5), new TreeNode(7, null, new TreeNode(8))));
		assert "[36, 36, 35, 33, 30, 26, 21, 15, 8]".equals(convertBST(root1).toString());
	}

	static TreeNode convertBST(TreeNode root) {
		convert(root, 0);
		return root;
	}

	private static int convert(TreeNode p, int s) {
		if (p == null)
			return s;
		final int rightSum = convert(p.right, s);
		p.val = p.val + rightSum;
		return convert(p.left, p.val);
	}
}