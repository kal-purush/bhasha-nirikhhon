package com.vmelnyk.geeks4geeks._20240122CourseSchedule;

import java.util.*;

class Solution {
  static int[] findOrder(int n, int m, ArrayList<ArrayList<Integer>> prerequisites) {
    // create Graph
    Map<Integer, List<Integer>> taskDependencies = new HashMap<>();
    for (int i = 0; i < n; i++) {
      taskDependencies.put(i, new ArrayList<>());
    }
    for (int i = 0; i < m; i++) {
      final List<Integer> prerequisite = prerequisites.get(i);
      taskDependencies.get(prerequisite.get(0)).add(prerequisite.get(1));
    }

    int[] dependencies = new int[n];
    for (int i = 0; i < taskDependencies.size(); i++) {
      for (Integer task : taskDependencies.get(i)) {
        dependencies[task]++;
      }
    }

    // BFS start
    Queue<Integer> bfs = new LinkedList<>();
    for (int tasks = 0; tasks < dependencies.length; tasks++) {
      if (dependencies[tasks] == 0) {
        bfs.add(tasks);
      }
    }

    int[] result = new int[n];
    int count = 0, cycle = 0;
    while (!bfs.isEmpty() && cycle++ < n) {
      int nextTask = bfs.poll();
      result[count++] = nextTask;
      for (Integer task : taskDependencies.get(nextTask)) {
        if (--dependencies[task] == 0) {
          bfs.add(task);
        }
      }
    }

    // checking graph contain cycle or not.
    if (cycle < n) { // if cycle contain then {ordering not possible}
      return new int[] {};
    }

    // return out according to the question.
    for (int i = 0; i < n / 2; i++) {
      int temp = result[i];
      result[i] = result[n - i - 1];
      result[n - i - 1] = temp;
    }

    return result;
  }
}