package problem114;

import common.TreeNode;

import java.util.LinkedList;
import java.util.List;

public final class TreeToLinkedList {

    public void flatten(final TreeNode root) {
        if (root == null)
            return;

        final List<TreeNode> preOrder = new LinkedList<>();
        preOrderToList(root, preOrder);

        TreeNode ptr = root;
        boolean rootNode = true;
        for (final TreeNode n : preOrder) {
            if (rootNode) {
                rootNode = false;
                continue;
            }
            ptr.left = null;
            ptr.right = n;
            ptr = n;
        }

        if (ptr != null) {
            ptr.right = null;
            ptr.left = null;
        }
    }

    private void preOrderToList(final TreeNode root, final List<TreeNode> list) {
        list.add(root);
        if (root.left != null)
            preOrderToList(root.left, list);
        if (root.right != null)
            preOrderToList(root.right, list);
    }

}