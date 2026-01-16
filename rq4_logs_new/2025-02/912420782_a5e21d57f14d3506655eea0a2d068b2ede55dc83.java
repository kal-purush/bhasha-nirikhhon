import java.util.*;

class Graph {
    private int vertices;
    private LinkedList<Edge>[] adjList;

    Graph(int vertices) {
        this.vertices = vertices;
        adjList = new LinkedList[vertices];
        for (int i = 0; i < vertices; i++) {
            adjList[i] = new LinkedList<>();
        }
    }

    void addEdge(int u, int v, int weight) {
        adjList[u].add(new Edge(v, weight));
        adjList[v].add(new Edge(u, weight));
    }

    void dijkstra(int source) {
        int[] dist = new int[vertices];
        boolean[] visited = new boolean[vertices];
        Arrays.fill(dist, Integer.MAX_VALUE);
        dist[source] = 0;

        for (int i = 0; i < vertices; i++) {
            int u = -1;
            for (int j = 0; j < vertices; j++) {
                if (!visited[j] && (u == -1 || dist[j] < dist[u])) {
                    u = j;
                }
            }

            if (dist[u] == Integer.MAX_VALUE) {
                break;
            }

            visited[u] = true;

            for (Edge edge : adjList[u]) {
                int v = edge.target;
                int weight = edge.weight;

                if (dist[u] + weight < dist[v]) {
                    dist[v] = dist[u] + weight;
                }
            }
        }

        System.out.println("최단 거리:");
        for (int i = 0; i < vertices; i++) {
            System.out.println("정점 " + i + "까지의 최단 거리: " + (dist[i] == Integer.MAX_VALUE ? "도달 불가" : dist[i]));
        }
    }

    class Edge {
        int target;
        int weight;

        Edge(int target, int weight) {
            this.target = target;
            this.weight = weight;
        }
    }
}

public class data_Structure {
    public static void main(String[] args) {
        Graph graph = new Graph(5);

        graph.addEdge(0, 1, 10);
        graph.addEdge(0, 4, 5);
        graph.addEdge(1, 2, 1);
        graph.addEdge(1, 4, 2);
        graph.addEdge(2, 3, 4);
        graph.addEdge(3, 0, 7);
        graph.addEdge(3, 2, 6);
        graph.addEdge(4, 1, 3);
        graph.addEdge(4, 2, 9);

        graph.dijkstra(0);
    }
}