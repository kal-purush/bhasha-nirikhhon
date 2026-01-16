package com.vmelnyk.geeks4geeks._20240123IsItTree;

import java.util.*;

class Solution {
  public boolean isTree(int n, int m, ArrayList<ArrayList<Integer>> edges) {
    // code here
    edges = new ArrayList<>(new HashSet<>(edges));
    if (n - 1 != edges.size()) {
      return false;
    }

    // create Graph
    List<List<Integer>> graph = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      graph.add(new ArrayList<>());
    }
    for (int i = 0; i < m; i++) {
      final List<Integer> edge = edges.get(i);
      graph.get(edge.get(0)).add(edge.get(1));
      graph.get(edge.get(1)).add(edge.get(0));
    }
    System.out.println(n + "," + m + "," + graph + "\n");

    boolean[] visited = new boolean[n];

    // Check for connectivity and cycles using DFS
    if (!dfs(graph, 0, -1, visited)) {
      return false;
    }

    // Check if all nodes are visited
    for (int i = 0; i < n; i++) {
      if (!visited[i]) {
        return false;
      }
    }

    return true;
  }

  private boolean dfs(List<List<Integer>> graph, int node, int parent, boolean[] visited) {
    visited[node] = true;

    for (int neighbor : graph.get(node)) {
      if (!visited[neighbor]) {
        if (!dfs(graph, neighbor, node, visited)) {
          return false;
        }
      } else if (neighbor != parent) {
        // If the neighbor is visited and not the parent, there is a cycle
        return false;
      }
    }

    return true;
  }
}

class Solution1 {
  public boolean isTree(int n, int m, ArrayList<ArrayList<Integer>> edges) {
    // code here
    edges = new ArrayList<>(new HashSet<>(edges));
    if (n - 1 != edges.size()) {
      return false;
    }

    // create Graph
    List<List<Integer>> graph = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      graph.add(new ArrayList<>());
    }
    for (int i = 0; i < m; i++) {
      final List<Integer> edge = edges.get(i);
      graph.get(edge.get(0)).add(edge.get(1));
      graph.get(edge.get(1)).add(edge.get(0));
    }

    System.out.println(graph);

    int[] dependencies = new int[n];
    int min = Integer.MAX_VALUE;
    for (int i = 0; i < graph.size(); i++) {
      for (Integer task : graph.get(i)) {
        dependencies[task]++;
        min = Math.min(min,dependencies[task]);
      }
    }
    System.out.println("dependencies=" + Arrays.toString(dependencies) + ",min="+min);

    // BFS start
    Queue<Integer> bfs = new LinkedList<>();
    for (int tasks = 0; tasks < dependencies.length; tasks++) {
      if (dependencies[tasks] == min) {
        bfs.add(tasks);
      }
    }

    System.out.println("bfs=" + bfs);

    int[] result = new int[n];
    int count = 0, cycle = 0;
    while (cycle++ < n && !bfs.isEmpty()) {
      int nextTask = bfs.poll();
      result[count++] = nextTask;
      for (Integer task : graph.get(nextTask)) {
        if (--dependencies[task] == 0) {
          bfs.add(task);
        }
      }
    }

    System.out.println("result=" + Arrays.toString(result) + ",cycles=" + cycle + ",n=" + n);
    System.out.println();
    // checking graph contain cycle or not.
    // if cycle contain then {ordering not possible}
    return !(cycle < n);
  }
}