package leetcode.java;

import java.util.HashSet;
import java.util.Set;

public class DistributeCandies_575 {
  public int distributeCandies(int[] candies) {
    int l = candies.length / 2;
    Set<Integer> s = new HashSet<>(l);
    for (int c : candies) {
      if (s.add(c)) {
        if (s.size() == l) return l;
      }
    }
    return s.size();
  }
}