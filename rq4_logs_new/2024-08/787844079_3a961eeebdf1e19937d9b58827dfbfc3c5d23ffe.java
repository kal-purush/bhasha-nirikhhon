//https://leetcode.com/problems/n-ary-tree-preorder-traversal/
package algorithms.easy;

import context.NaryNode;

import java.util.ArrayList;
import java.util.List;

public class NaryTreePreorderTraversal {
    List<Integer> answer = new ArrayList<>();

    public List<Integer> preorder(NaryNode root) {
        traversal(root);
        return answer;
    }

    public void traversal(NaryNode node) {
        if (node == null) return;
        answer.add(node.val);
        for (NaryNode child : node.children)
            traversal(child);
        return;
    }
}