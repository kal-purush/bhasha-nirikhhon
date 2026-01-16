package com.vmelnyk.leetcode._547NumberOfProvinces;

import java.util.*;

class Solution {
  public int findCircleNum(int[][] isConnected) {
    final int n = isConnected.length;
    final boolean[] check = new boolean[n];
    int count = 0;
    for (int i = 0; i < n; i++) {
      if (!check[i]) {
        count++;
        checkConnection(isConnected, check, i);
      }
    }
    return count;
  }

  void checkConnection(int[][] isConnected, boolean[] check, int i) {
    final int n = isConnected.length;
    check[i] = true;
    for (int j = 0; j < n; j++) {
      if (!check[j] && isConnected[i][j] == 1) {
        checkConnection(isConnected, check, j);
      }
    }
  }
}

class MySolution {
  public int findCircleNum(int[][] isConnected) {
    final int n = isConnected.length;

    final Map<Integer, List<Integer>> links = new HashMap<>();
    for (int i = 0; i < n; i++) {
      for (int j = i + 1; j < n; j++) {
        if (isConnected[i][j] == 1) {
          links.computeIfAbsent(i, t -> new LinkedList<>()).add(j);
          links.computeIfAbsent(j, t -> new LinkedList<>()).add(i);
        }
      }
    }

    Set<Integer> visited = new HashSet<>();
    Queue<Integer> q = new LinkedList<>();
    int circles = 0;

    for (int i = 0; i < n; i++) {
      if (visited.add(i)) {
        if (links.containsKey(i)) {
          q.offer(i);

          while (!q.isEmpty()) {
            final Integer town = q.poll();
            final List<Integer> link = links.get(town);
            if (link == null) {
              continue;
            }
            for (Integer t : link) {
              if (visited.add(t)) {
                q.offer(t);
              }
            }
          }
        }
        circles++;
      }
    }

    return circles;
  }
}