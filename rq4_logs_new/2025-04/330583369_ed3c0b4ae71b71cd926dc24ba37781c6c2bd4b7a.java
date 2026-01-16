package com.horadrim.staff.alg.graph;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class DirectedGraph {
    public DirectedGraph(int vertices) {
        _vertices = vertices;
        _edges = new ArrayList<>(_vertices);
        for (int i = 0; i < _vertices; ++i) {
            _edges.add(null);
        }
    }

    public void addEdge(int source, int dest) {
        if (_edges.get(source) == null) {
            LinkedList<Integer> edge = new LinkedList<>();
            edge.offer(dest);
            _edges.set(source, edge);
        }
        else
        {
            LinkedList<Integer> edge = _edges.get(source);
            edge.offer(dest);
        }
    }

    public boolean validPath(int source, int dest) {
        List<LinkedList<Integer>> adj = new ArrayList<>(_edges);

        boolean[] visited = new boolean[Math.max(source, Math.max(_vertices, dest)) + 1];
        Queue<Integer> queue = new ArrayDeque<>();
        queue.offer(source);

        visited[source] = true;
        while (!queue.isEmpty()) {
            int vertex = queue.poll();
            if (vertex == dest) {
                break;
            }

            if (adj.get(vertex) == null) {
                continue;
            }

            for (int next : adj.get(vertex)) {
                if (!visited[next]) {
                    queue.offer(next);
                    visited[next] = true;
                }
            }
        }

        return visited[dest];
    }

    private boolean dfs(boolean[] visited, List<LinkedList<Integer>> adj, int source, int dest) {
        if (source == dest) {
            return true;
        }
        visited[source] = true;

        if (adj.get(source) == null) {
            return false;
        }

        for (int next : adj.get(source)) {
            if (!visited[next] && dfs(visited, adj, next, dest)) {
                return true;
            }
        }

        return false;
    }

    public List<Integer> topologicalSortKahn() {
        int[] in_degree = new int[_vertices];
        Queue<Integer> q = new ArrayDeque<>();
        List<Integer> topo_order = new ArrayList<>();

        // 计算每个节点的入度
        for (int i = 0; i < _vertices; ++i) {
            if (_edges.get(i) == null) {
                continue;
            }

            for (int neighbor : _edges.get(i)) {
                in_degree[neighbor]++;
            }
        }

        // 将所有入度为 0 的节点加入队列
        for (int i = 0; i < _vertices; ++i) {
            if (in_degree[i] == 0) {
                q.offer(i);
            }
        }

        while(!q.isEmpty()) {
            int u = q.poll();
            topo_order.add(u);

            if (_edges.get(u) == null) {
                continue;
            }

            for (int neighbor : _edges.get(u)) {
                in_degree[neighbor]--;
                if (in_degree[neighbor] == 0) {
                    q.offer(neighbor);
                }
            }
        }

        return (topo_order.size() == _vertices ? topo_order : null);
    }

    public boolean validPathByDFS(int source, int dest) {
        boolean[] visited = new boolean[Math.max(source, Math.max(_vertices, dest)) + 1];
        List<LinkedList<Integer>> adj = new ArrayList<>(_edges);

        return dfs(visited, adj, source, dest);
    }

    public static DirectedGraph fromMatrix(int [][] adjMatrix) {
        DirectedGraph graph = new DirectedGraph(adjMatrix.length);
        return graph;
    }

    List<Integer> adjMatrixBFS(int [][] adjMatrix, int source, int dest) {
        int n = adjMatrix.length;
        int [] prev = new int[n + 1];
        Arrays.fill(prev, -1);
        Queue<Integer> q = new ArrayDeque<>();
        q.offer(source);

        while(!q.isEmpty()) {
            int current = q.poll();
            if (current == dest) {
                break;
            }

            for (int i = 0; i < n; ++i) {
                if (adjMatrix[current][i] == 1 && prev[i] == -1) {
                    q.offer(i);
                    prev[i] = current;
                }
            }
        }

        if (prev[dest] != -1) {
            List<Integer> path = new ArrayList<>();
            for (int at = dest; at != -1; at = prev[at]) {
                path.add(at);
            }
            return path.reversed();
        }

        return null;
    }

    public int[][] toMatrix() {
        int[][] matrix = new int[_vertices + 1][_vertices + 1];
        for (int i = 0; i < _edges.size(); ++i) {
            if (_edges.get(i) == null) {
                continue;
            }

            for (int dest : _edges.get(i)) {
                matrix[i][dest] = 1;
            }
        }

        return matrix;
    }

    private List<LinkedList<Integer>> _edges;
    private int _vertices;
}