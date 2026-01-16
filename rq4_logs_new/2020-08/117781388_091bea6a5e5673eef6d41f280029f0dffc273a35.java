package BinaryTree;

import java.util.*;

public class VerticalOrderTraversalOfABinaryTree {
    public List<List<Integer>> verticalTraversal(TreeNode root) {
        List<List<Integer>> result = new ArrayList<>();
        if (root == null) {
            return result;
        }

        Map<Integer, List<Element>> map = new TreeMap<>();
        Queue<Integer> queueXPosition = new LinkedList<>();
        Queue<Element> queue = new LinkedList<>();
        queue.offer(new Element(root, 0));
        queueXPosition.offer(0);

        while (!queue.isEmpty()) {
            Element element = queue.poll();
            int X = queueXPosition.poll();
            if (!map.containsKey(X)) {
                map.put(X, new ArrayList<>(Arrays.asList(element)));
            } else {
                map.get(X).add(element);
            }

            if (element.node.left != null) {
                queue.offer(new Element(element.node.left, element.Y - 1));
                queueXPosition.offer(X - 1);
            }

            if (element.node.right != null) {
                queue.offer(new Element(element.node.right, element.Y - 1));
                queueXPosition.offer(X + 1);
            }
        }

        for (int n : map.keySet()) {
            Collections.sort(map.get(n), new Comparator<Element>() {
                @Override
                public int compare(Element o1, Element o2) {
                    if (o1.Y > o2.Y) {
                        return -1;
                    }

                    if (o1.Y == o2.Y) {
                        return o1.node.val - o2.node.val;
                    }

                    return 1;
                }
            });

            List<Integer> sortedList = new ArrayList<>();
            for (int i = 0; i < map.get(n).size(); i++) {
                sortedList.add(map.get(n).get(i).node.val);
            }

            result.add(sortedList);
        }

        return result;
    }

    class Element {
        TreeNode node;
        int Y;

        public Element(TreeNode node, int Y) {
            this.node = node;
            this.Y = Y;
        }
    }
}