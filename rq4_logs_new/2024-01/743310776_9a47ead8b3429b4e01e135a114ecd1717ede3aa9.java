package com.vmelnyk.geeks4geeks._20240118TopKNumbersInStream;

import java.util.*;

class Solution {
  public static ArrayList<ArrayList<Integer>> kTop(int[] a, int N, int K) {
    // code here
    final Comparator<Map.Entry<Integer, Integer>> entryComparator =
        (o1, o2) -> {
          if (o1.getValue() == o2.getValue()) {
            return o1.getKey().compareTo(o2.getKey());
          }
          return -o1.getValue().compareTo(o2.getValue());
        };

    final Map<Integer, Integer> counts = new HashMap<>();
    final ArrayList<ArrayList<Integer>> result = new ArrayList<>();
    for (int i = 0; i < N; i++) {
      int num = a[i];
      if (num < 1) {
        continue;
      }

      counts.put(num, counts.getOrDefault(num, 0) + 1);

      List<Map.Entry<Integer, Integer>> l = new ArrayList<>(counts.entrySet());
      l.sort(entryComparator);
      ArrayList<Integer> iteration = new ArrayList<>();
      for (Map.Entry<Integer, Integer> e : l) {
        iteration.add(e.getKey());
        if (iteration.size() == K) {
          break;
        }
      }
      result.add(iteration);
    }

    return result;
  }
}