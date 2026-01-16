package algorithm.greedy;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Find the shortest path and 2nd shortest path using Dijkstra.
 **
 * https://www.lavivienpost.com/shortest-path-and-2nd-shortest-path-using-dijkstra-code/
 *
 */
public class ShortestPathAnd2ndShortestDijkstras {
    static final int NO_PARENT = -1;

    public static void main(String[] args) {
        /*
         *      0
         *     / \
         * (1)/   \(1)
         *   /     \
         *  3--(4)--1--(1)--2
         */

        int[][] adjacencyMatrix = new int[][] {
                { 0, 1, 0, 1},
                { 1, 0, 1, 4},
                { 0, 1, 0, 0},
                { 1, 4, 0, 0}
        };
        int src = 2, dest = 3;
        Set<Integer> path = shortestPath(adjacencyMatrix, 2, 3);
        System.out.println("Shortest path: " + path);
    }

    /**
     *
     * @param adjacencyMatrix
     * @param src
     * @param dest
     * @return
     */
    static Set<Integer> shortestPath(int[][] adjacencyMatrix, int src, int dest) {
        int n = adjacencyMatrix.length;     // number of vertices
        int[] shortest = new int[n];        // store the shortest distance to every vertex in graph
        boolean[] visited = new boolean[n]; // indicate if the vertex is visited
        int[] parents = new int[n];         // store the parent vertex in the shortest path

        // initialize all shortest distances as Infinite and none visited vertex
        for (int i = 0; i < n; i++) {
            shortest[i] = Integer.MAX_VALUE;
            visited[i] = false;
        }

        // we start from source vertex, the distance of source vertex from itself is always 0
        shortest[src] = 0;
        parents[src] = NO_PARENT;

        // Find the shortest path for all vertices
        for (int i = 1; i < n; i++) {
            // Pick the minimum distance vertex from the set of vertices not yet visited.
            // 'pre' is always equal to srcNode in first iteration.
            int pre = minDistance(shortest, visited);

            visited[pre] = true;

            // Update the shortest distance of the adjacent vertices and parent vertex in the shortest path
            updateShortest(pre, shortest, parents, adjacencyMatrix);
        }

        // after found the shortest path for all vertices, we find the shortest path for the given vertex.
        Set<Integer> shortestPath = new LinkedHashSet<>();
        addPathRecursive(dest, parents, shortestPath);
        return shortestPath;
    }

    /**
     * Find 2nd shortest path is a Find kth problem. It can be achieved by using K_shortest_path_routing or Yen’s_algorithm.
     *  * The steps ares:
     *  *   - First find the shortest path using Dijkstra
     *  *   - Second, remove each edge in the shortest path and find the shortest path again
     *  *   - Finally, compare and return the shortest path among them as the second shortest path.
     *
     * @param adjacencyMatrix
     * @param src
     * @param dest
     * @param shortestPath
     */
    static void find2ndShortest(int[][] adjacencyMatrix, int src, int dest, Set<Integer> shortestPath) {
        int preV = -1, preS = -1, preD = -1; //store previous vertex's data
        List<Integer> list = new ArrayList<Integer>(shortestPath);
        for (int i = 0; i < list.size()-1 ; i++) {
            //get source and destination for each path in shortest path
            int s = list.get(i);
            int d = list.get(i + 1);
            if (preV != -1) {//resume the previous path
                adjacencyMatrix[preS][preD] = preV;
                adjacencyMatrix[preD][preS] = preV;
            }
            //record the previous data for recovery
            preV = adjacencyMatrix[s][d];
            preS = s;
            preD = d;
            //remove this path
            adjacencyMatrix[s][d] = 0;
            adjacencyMatrix[d][s] = 0;
            //re-calculate
            shortestPath(adjacencyMatrix, src , dest);
        }
    }

    private static void addPathRecursive(int dest, int[] parents, Set<Integer> shortestPath) {
        if (dest == NO_PARENT) {
            return;
        }

        addPathRecursive(parents[dest], parents, shortestPath);
        shortestPath.add(dest);
    }

    /**
     * Find the minimum distance vertex from the set of vertices NOT visited.
     * @param shortest
     * @param visited
     * @return node index which is not visited and has the minimum distance.
     */
    private static int minDistance(int[] shortest, boolean[] visited) {
        int minDistNode = -1;
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < shortest.length; i++) {
            if (!visited[i] && min > shortest[i]) {
                min = shortest[i];
                minDistNode = i;
            }
        }
        return minDistNode;
    }

    private static void updateShortest(int src, int[] shortest, int[] parents, int[][] adjacencyMatrix) {
        for (int i = 0; i < shortest.length; i++) {
            int dist = adjacencyMatrix[src][i];
            if (dist > 0 && (dist + shortest[src]) < shortest[i]) {
                shortest[i] = dist + shortest[src];
                parents[i] = src;
            }
        }
    }
}