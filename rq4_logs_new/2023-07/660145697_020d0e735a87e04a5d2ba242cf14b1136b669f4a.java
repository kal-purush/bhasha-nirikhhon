package rish.leets.grind.medium;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Grind 75 : Week 3
 * 
 * Problem #: 133
 * Problem link : https://leetcode.com/problems/clone-graph/
 * 
 * Date Attempted: 18/07/2023
 * 
 * @author Rishabh Soni
 *
 */
public class CloneGraph {

    class Node {
        public int val;
        public List<Node> neighbors;

        public Node() {
            val = 0;
            neighbors = new ArrayList<Node>();
        }

        public Node(int _val) {
            val = _val;
            neighbors = new ArrayList<Node>();
        }

        public Node(int _val, ArrayList<Node> _neighbors) {
            val = _val;
            neighbors = _neighbors;
        }
    }

    public Node cloneGraph(Node node) {

        if (node == null) {
            return null;
        }

        if (node.neighbors.isEmpty()) {
            return new Node(node.val);
        }

        Map<Integer, Node> map = new HashMap<>();
        cloneGraphInternal(node, map);
        return map.get(node.val);
    }

    private Node cloneGraphInternal(Node node, Map<Integer, Node> map) {

        // Create a clone
        Node clone = new Node(node.val);
        map.put(node.val, clone);

        // Check for neighbours
        for (Node n : node.neighbors) {

            Node clonedNb = map.getOrDefault(n.val, cloneGraphInternal(n, map));

            clone.neighbors.add(clonedNb);
        }

        return clone;
    }

}