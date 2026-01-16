//https://leetcode.com/problems/maximum-depth-of-n-ary-tree/description/
package algorithms.easy;

import context.NaryNode;

import java.util.ArrayList;
import java.util.List;

public class MaximumDepthOfNaryTree {
    int depth;

    public static void main(String[] args) {
        MaximumDepthOfNaryTree maxDepth = new MaximumDepthOfNaryTree();
        NaryNode n1 = new NaryNode(1);
        NaryNode n2 = new NaryNode(2);
        NaryNode n3 = new NaryNode(3);
        NaryNode n4 = new NaryNode(4);
        NaryNode n5 = new NaryNode(5);
        NaryNode n6 = new NaryNode(6);
        List<NaryNode> list1 = new ArrayList<>();
        list1.add(n2);
        list1.add(n3);
        list1.add(n4);
        n1.children = list1;

        List<NaryNode> list2 = new ArrayList<>();
        list2.add(n5);
        list2.add(n6);
        n3.children = list2;

        System.out.println("Output:\t" + maxDepth.maxDepthOptimized(n1));
        System.out.println("Output:\t" + maxDepth.maxDepthNonOptimized(n1));
    }

    public int maxDepthOptimized(NaryNode root) {
        if (root == null) return 0;
        int depthOptimized = 1;
        for (NaryNode node : root.children)
            depthOptimized = Math.max(depthOptimized, 1 + maxDepthOptimized(node));
        return depthOptimized;
    }

    public int maxDepthNonOptimized(NaryNode root) {
        depth = 0;
        dfs(root, 1);
        return depth;
    }

    public int dfs(NaryNode node, int dep) {
        if (node == null) return dep;
        depth = Math.max(depth, dep);
        for (NaryNode child : node.children)
            dfs(child, dep + 1);
        return dep;
    }
}