package problem102;

import common.TreeNode;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public final class BinaryTreeLevelOrderTraversal {

    public List<List<Integer>> levelOrder(final TreeNode root) {
        if (root == null)
            return List.of();

        final List<List<Integer>> result = new LinkedList<>();
        final Queue<TreeNode> q = new LinkedList<>();
        q.offer(root);

        while (!q.isEmpty()) {
            final List<Integer> level = new ArrayList<>();
            final int qSize = q.size();

            for (int i = 0; i < qSize; i++) {
                final TreeNode n = q.poll();
                level.add(n.val);

                if (n.left != null)
                    q.offer(n.left);

                if (n.right != null)
                    q.offer(n.right);
            }
            result.add(level);
        }

        return result;
    }

}