package com.vmelnyk.leetcode._1466ReorderRoutesToMakeAllPathsLeadToTheCityZero;

import java.util.LinkedList;
import java.util.Stack;

class Solution {
  public int minReorder(int n, int[][] connections) {
    int rows = connections.length;
    boolean[] reach = new boolean[n];
    reach[0] = true;
    Stack<Integer> lvl1 = new Stack<>();
    int count = 0;
    for (int i = 0; i < rows; i++) {
      count += getCount(connections, reach, i, lvl1);
    }
    Stack<Integer> lvl2 = new Stack<>();
    while (!lvl1.isEmpty() || !lvl2.isEmpty()) {
      while (!lvl1.isEmpty()) {
        count += getCount(connections, reach, lvl1.pop(), lvl2);
      }
      while (!lvl2.isEmpty()) {
        count += getCount(connections, reach, lvl2.pop(), lvl1);
      }
    }
    return count;
  }

  private static int getCount(
      int[][] connections, boolean[] reach, int town, Stack<Integer> postpone) {
    int count = 0;
    if (reach[connections[town][0]]) {
      count++;
      reach[connections[town][1]] = true;
    } else if (reach[connections[town][1]]) {
      reach[connections[town][0]] = true;
    } else {
      postpone.push(town);
    }
    return count;
  }
}

class Solution1 {
  public int minReorder(int n, int[][] connections) {
    LinkedList<int[]>[] graph = new LinkedList[n];
    for (int i = 0; i != n; ++i) {
      graph[i] = new LinkedList<>(); // create graph
    }
    for (int[] c : connections) { // put all edges
      graph[c[0]].add(new int[] {c[1], 1}); // index[1] == 1 - road is present
      graph[c[1]].add(new int[] {c[0], 0}); // index[1] == 0 - road is absent
    }

    int[] visited = new int[n];
    int reorderRoads = 0;
    LinkedList<Integer> q = new LinkedList<>(); // queue for BFS
    q.add(0);
    while (!q.isEmpty()) { // BFS
      int city = q.poll();
      if (visited[city] == 1) {
        continue;
      }
      visited[city] = 1;

      for (int[] neighbor : graph[city])
        if (visited[neighbor[0]] == 0) {
          q.add(neighbor[0]);
          if (neighbor[1] == 1) {
            ++reorderRoads;
          }
        }
    }

    return reorderRoads;
  }
}