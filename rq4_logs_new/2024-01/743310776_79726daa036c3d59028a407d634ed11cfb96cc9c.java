package com.vmelnyk.leetcode._399EvaluateDivision;

import java.util.*;

class Solution {
  private record Node(String key, double val) {}

  public double[] calcEquation(
      List<List<String>> equations, double[] values, List<List<String>> queries) {
    final Map<String, List<Node>> graph = createGraph(equations, values);
    final int queriesCount = queries.size();
    final double[] result = new double[queriesCount];

    for (int i = 0; i < queriesCount; i++) {
      final String ci = queries.get(i).get(0);
      final String di = queries.get(i).get(1);
      result[i] = dfs(ci, di, new HashSet<>(), graph);
    }

    return result;
  }

  private double dfs(String ci, String di, Set<String> visited, Map<String, List<Node>> graph) {
    if (!graph.containsKey(ci) || !graph.containsKey(di)) {
      return -1.0;
    }

    if (ci.equals(di)) {
      return 1.0;
    }

    visited.add(ci);
    for (Node n : graph.get(ci)) {
      if (!visited.contains(n.key)) {
        double result = dfs(n.key, di, visited, graph);
        if (result != -1.0) {
          return result * n.val;
        }
      }
    }

    return -1.0;
  }

  private Map<String, List<Node>> createGraph(List<List<String>> equations, double[] values) {
    final Map<String, List<Node>> graph = new HashMap<>();

    for (int i = 0; i < equations.size(); i++) {
      final String src = equations.get(i).get(0);
      final String dst = equations.get(i).get(1);

      graph.putIfAbsent(src, new ArrayList<>());
      graph.putIfAbsent(dst, new ArrayList<>());

      graph.get(src).add(new Node(dst, values[i]));
      graph.get(dst).add(new Node(src, 1.0 / values[i]));
    }

    return graph;
  }
}