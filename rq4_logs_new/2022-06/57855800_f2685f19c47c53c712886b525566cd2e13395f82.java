package com.github.lucaseo90.algo.days21.day7;

import java.util.LinkedList;
import java.util.Queue;

public class PopulatingNextRightPointers {

    public Node connect(Node root) {
        if (root == null) {
            return null;
        }

        Queue<NodeLevel> queue = new LinkedList<>();
        queue.add(new NodeLevel(0, root));

        while (!queue.isEmpty()) {
            NodeLevel nl = queue.poll();
            Node node = nl.node;
            int level = nl.level;

            if (queue.peek() != null && level == queue.peek().level) {
                node.next = queue.peek().node;
            }

            if (node.left != null) {
                queue.add(new NodeLevel(level + 1, node.left));
            }

            if (node.right != null) {
                queue.add(new NodeLevel(level + 1, node.right));
            }
        }

        return root;
    }

    private class NodeLevel {
        int level;
        Node node;

        public NodeLevel(int level, Node node) {
            this.level = level;
            this.node = node;
        }
    }
}