public class Solution {
    public int findChampion(int n, int[][] edges) {
        int[] inDegree = new int[n];
        List<List<Integer>> graph = new ArrayList<>();
        
        for (int i = 0; i < n; i++) {
            graph.add(new ArrayList<>());
        }
        
        for (int[] edge : edges) {
            int u = edge[0], v = edge[1];
            graph.get(u).add(v);
            inDegree[v]++;
        }
        
        Queue<Integer> queue = new LinkedList<>();
        for (int i = 0; i < n; i++) {
            if (inDegree[i] == 0) queue.add(i);
        }
        
        while (queue.size() > 1) {
            int team = queue.poll();
            for (int neighbor : graph.get(team)) {
                inDegree[neighbor]--;
                if (inDegree[neighbor] == 0) queue.add(neighbor);
            }
        }
        
        int candidate = queue.poll();
        if (candidate == -1) return -1;
        
        boolean[] visited = new boolean[n];
        dfs(graph, candidate, visited);
        for (int i = 0; i < n; i++) {
            if (!visited[i]) return -1;
        }
        return candidate;
    }
    
    private void dfs(List<List<Integer>> graph, int node, boolean[] visited) {
        if (visited[node]) return;
        visited[node] = true;
        for (int neighbor : graph.get(node)) {
            dfs(graph, neighbor, visited);
        }
    }
}