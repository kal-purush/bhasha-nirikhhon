package datastructures.graphs;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * The class which represents the simple graph based on
 *
 * @param <N> the type of the node key in the graph
 * @param <E> the type of value stored in the particular node
 */
class AdjacencyMatrixGraph<N extends Number, E extends Comparable<E>> {
    // Container for all nodes in the graph
    private final ArrayList<N> nodes = new ArrayList<>();
    // Adjacency matrix. Contains: node_0 -- pair(node, weightOfEdge)_0 -- ... -- pair(...)_k
    //                             node_1 -- ...
    //                             ...
    //                             node_n -- ...
    private ArrayList<Pair>[] matrix = null;

    /**
     * Fill the matrix with fixed vertices number.
     *
     * @param size number of vertices in the graph
     */
    public void fillMatrix(int size) {
        matrix = new ArrayList[size];
        for (int i = 0; i < size; i++) {
            matrix[i] = new ArrayList<>();
            for (int j = 0; j < size; j++) {
                matrix[i].add(null);
            }
        }
    }

    /**
     * Add node to the graph.
     *
     * @param node The variable of given by user type.
     */
    public void addNode(N node) {
        nodes.add(node);
    }

    /**
     * Add edge between two nodes. (Simply speaking, fill the adjacency matrix)
     *
     * @param source      starting node
     * @param destination destination node
     * @param weight      weight of this particular edge
     */
    public void addEdge(N source, N destination, E weight, E bandwidth) {
        matrix[(Integer) source].set((Integer) destination, new Pair(destination, weight, bandwidth));
    }

    /**
     * Find the shortest path in the graph according to the constraint about min. bandwidth.
     *
     * @param src       the starting node
     * @param dest      the finishing node
     * @param bandwidth min bandwidth for every edge
     */
    public void findShortestPath(N src, N dest, E bandwidth) {
        // Check for the forest
        if (!isNodeReachable(src, dest)) {
            System.out.println("IMPOSSIBLE");
            return;
        }

        // Implement Dijkstra's algorithm for all vertices
        ArrayList<N> taken = new ArrayList<>();
        HashMap<N, Double> distance = new HashMap<>();

        for (N node : nodes) {
            distance.put(node, Double.MAX_VALUE);
        }

        distance.replace(src, 0.0);

        while (taken.size() != matrix.length - 1) {
            N curNode = getMinDistance(taken, distance);

            if (curNode == null && taken.contains(dest)) {
                break;
            }

            if (curNode == null) {
                System.out.println("IMPOSSIBLE");
                System.exit(0);
            }

            taken.add(curNode);

            for (Pair edge : matrix[(Integer) curNode]) {
                if (edge == null) continue;

            }
        }

        // Find the shortest path according go the already found min path from source to dest, equals to
        // distance.get(dest)
        StringBuilder stringBuilder = new StringBuilder();

        ArrayList<N> path = new ArrayList<>();
        path.add(src);

        System.out.println(stringBuilder);
    }

    /**
     * Traverse the graph from the source node to the destination node, recursively.
     * If we find the path and the path's length is the smallest one (Already known
     * from the Dijkstra's algorithm) - print all data and return from function.
     *
     * @param curNode       curNode in pathway
     * @param dest          destination of path
     * @param minLen        minLen known from the Dijkstra's algorithm
     * @param bandwidth     main constraint of the whole program
     * @param curLen        curLength of the path
     * @param curBw         curBandwidth of the path
     * @param visited       which nodes already taken in the path
     * @param stringBuilder the report formation for the whole function
     */
    private void traverse(N curNode, N dest, double minLen, double bandwidth, double curLen, double curBw,
                          ArrayList<N> visited, StringBuilder stringBuilder) {
        if (visited.contains(dest) && curLen == minLen) {
            stringBuilder.append(visited.size()).append(" ");
            stringBuilder.append((int) minLen).append(" ");
            stringBuilder.append((int) curBw).append("\n");
            visited.forEach(n -> stringBuilder.append((Integer) n + 1).append(" "));
            return;
        }

        if (curLen > minLen) return;

        for (Pair pair : matrix[(Integer) curNode]) {
            if (pair == null) continue;
        }
    }

    /**
     * Get min distance from the possible moves while using Dijkstra's algorithm.
     *
     * @param taken    already taken nodes
     * @param distance distance to nodes path can possibly go
     * @return node which will be taken as a node with min distance
     */
    private N getMinDistance(ArrayList<N> taken, HashMap<N, Double> distance) {
        Double minVal = Double.MAX_VALUE;
        N minNode = null;

        for (N node : distance.keySet()) {
            if (!taken.contains(node)) {

                if (distance.get(node) < minVal) {
                    minVal = distance.get(node);
                    minNode = node;
                }

            }
        }

        return minNode;
    }

    /**
     * Check is destination node reachable from the source node
     *
     * @param source      source node
     * @param destination destination node
     * @return true if reachable; Otherwise, false
     */
    private boolean isNodeReachable(N source, N destination) {
        HashMap<N, Boolean> visited = new HashMap<>();
        for (N node : nodes) {
            visited.put(node, false);
        }

        visited.replace(source, true);

        makeMove(source, visited);

        return visited.get(destination);
    }

    /**
     * Supporting recursive function for reachability checking function
     *
     * @param node    current node
     * @param visited already visited nodes
     */
    private void makeMove(N node, HashMap<N, Boolean> visited) {
        for (Pair neighbour : matrix[(Integer) node]) {
            if (neighbour == null) continue;
            if (visited.get(neighbour.destination)) {
                continue;
            }
            visited.replace(neighbour.destination, true);
            makeMove(neighbour.destination, visited);
        }
    }

    /**
     * Class which represents the pair of node + edge.length().
     * In addition, there is exists bandwidth of single edge.
     */
    class Pair {
        private final N destination;
        private final E length;

        private final E bandWidth;

        Pair(N destinationInp, E lengthInp, E bandWidthInp) {
            this.destination = destinationInp;
            this.length = lengthInp;
            this.bandWidth = bandWidthInp;
        }
    }
}