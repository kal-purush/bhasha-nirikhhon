package com.vmelnyk.leetcode._118PascalsTriangle;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

class Solution {
  public List<List<Integer>> generate(int numRows) {
    final List<List<Integer>> result = new ArrayList<>();
    final BiFunction<List<Integer>, Integer, Integer> func =
        (l, i) -> {
          int size = l.size();

          return i < 0 || i >= size ? 0 : l.get(i);
        };

    List<Integer> prevRowNumbers = List.of(1);
    for (int row = 1; row <= numRows; row++) {
      List<Integer> rowNumbers = new ArrayList<>(1);

      for (int n = 0; n < row; n++) {
        rowNumbers.add(func.apply(prevRowNumbers, n - 1) + func.apply(prevRowNumbers, n));
      }

      result.add(rowNumbers);
      prevRowNumbers = rowNumbers;
    }

    System.out.println(result);

    return result;
  }
}