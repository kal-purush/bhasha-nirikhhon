package come.eClass5_BST_Sorting;

/**
 * leetcode 776 split BST
 */
public class Q5_1_SplitBinarySearchTree {
    public class TreeNode {
        public int val;
        public TreeNode left;
        public TreeNode right;
        public TreeNode(int key) {
            val = key;
        }
    }

    public TreeNode[] splitBST(TreeNode root, int V) {
        if (root == null) {
            return new TreeNode[] {null, null};
        }

        if (root.val > V) {
            TreeNode[] nodes = splitBST(root.left, V);
            TreeNode left = nodes[0];
            TreeNode right = nodes[1];
            root.left = right;
            return new TreeNode[] {left, root};
        } else if (root.val < V) {
            TreeNode[] nodes = splitBST(root.right, V);
            TreeNode left = nodes[0];
            TreeNode right = nodes[1];
            root.right = left;
            return new TreeNode[] {root, right};
        } else {	// root.val == V
            TreeNode right = root.right;
            root.right = null;
            return new TreeNode[] {root, right};
        }
    }
}