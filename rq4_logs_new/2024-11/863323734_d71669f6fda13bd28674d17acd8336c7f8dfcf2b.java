public class Solution {
    public int[][] validArrangement(int[][] pairs) {
        // Step 1: Build the graph and degree information
        Map<Integer, Queue<Integer>> graph = new HashMap<>();
        Map<Integer, Integer> outDegree = new HashMap<>();
        Map<Integer, Integer> inDegree = new HashMap<>();

        // Build graph and track degrees
        for (int[] pair : pairs) {
            int from = pair[0], to = pair[1];
            graph.computeIfAbsent(from, k -> new LinkedList<>()).add(to);
            outDegree.put(from, outDegree.getOrDefault(from, 0) + 1);
            inDegree.put(to, inDegree.getOrDefault(to, 0) + 1);
        }

        // Step 2: Find the starting node
        int start = pairs[0][0];
        for (int node : graph.keySet()) {
            if (outDegree.getOrDefault(node, 0) - inDegree.getOrDefault(node, 0) == 1) {
                start = node;
                break;
            }
        }

        // Step 3: Perform Hierholzer's algorithm
        List<int[]> result = new ArrayList<>();
        Stack<Integer> stack = new Stack<>();
        stack.push(start);

        while (!stack.isEmpty()) {
            int node = stack.peek();
            if (graph.containsKey(node) && !graph.get(node).isEmpty()) {
                int nextNode = graph.get(node).poll();
                stack.push(nextNode);
            } else {
                stack.pop();
                if (!stack.isEmpty()) {
                    result.add(new int[]{stack.peek(), node});
                }
            }
        }

        // Reverse the result to get the correct order
        Collections.reverse(result);
        return result.toArray(new int[result.size()][]);
    }
}