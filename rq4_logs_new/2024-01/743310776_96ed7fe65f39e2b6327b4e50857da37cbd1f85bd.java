package com.vmelnyk.leetcode._841KeysAndRooms;

import java.util.*;

class Solution {
  public boolean canVisitAllRooms(List<List<Integer>> rooms) {

    Set<Integer> foundKeys = new HashSet<>();
    Queue<Integer> q = new LinkedList<>();

    foundKeys.add(0);
    q.add(0);

    while (!q.isEmpty()) {
      Integer key = q.poll();
      for (Integer k : rooms.get(key)) {
        if (foundKeys.add(k)) {
          q.offer(k);
        }
      }
    }

    return foundKeys.size() == rooms.size();
  }
}