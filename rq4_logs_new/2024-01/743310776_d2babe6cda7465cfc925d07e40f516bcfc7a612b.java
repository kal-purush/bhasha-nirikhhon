package com.vmelnyk.leetcode._119PascalsTriangleII;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

class Solution {
  public List<Integer> getRow(int rowIndex) {
    List<Integer> rowNumbers = new ArrayList<>();
    long v = 1;
    rowNumbers.add(1);
    for (int i = 1; i <= rowIndex; i++) {
      v = v * (rowIndex - i + 1);
      v = v / i;
      rowNumbers.add((int) v);
    }

    //System.out.println(rowNumbers);

    return rowNumbers;
  }

  public List<Integer> getRow1(int rowIndex) {
    final BiFunction<List<Integer>, Integer, Integer> func =
        (l, i) -> {
          int size = l.size();
          return i < 0 || i >= size ? 0 : l.get(i);
        };

    List<Integer> prevRowNumbers = List.of(1);
    for (int row = 1; row <= rowIndex; row++) {
      List<Integer> rowNumbers = new ArrayList<>(row);

      for (int n = 0; n < row + 1; n++) {
        rowNumbers.add(func.apply(prevRowNumbers, n - 1) + func.apply(prevRowNumbers, n));
      }

      prevRowNumbers = rowNumbers;
    }

    // System.out.println(prevRowNumbers);

    return prevRowNumbers;
  }

  public List<Integer> getRow2(int rowIndex) {

    List<Integer> rowNumbers = new ArrayList<>(rowIndex + 1);

    final long factN = fact(rowIndex);

    for (int k = 0; k <= rowIndex; k++) {
      final long factN_R = fact(rowIndex - k);
      final long factR = fact(k);
      final int combine = (int) (factN / factN_R / factR);
      rowNumbers.add(combine);
    }

    System.out.println(rowNumbers);

    return rowNumbers;
  }

  public static long fact(int a) {
    long f = 1L;
    for (int i = 2; i <= a; i++) f = f * i;
    return f;
  }

  public static int combine(int n, int r) {
    if (n < r) {
      return 0;
    }

    final double factN_R = fact(n - r);
    final double factR = fact(r);
    final double factN = fact(n);

    return (int) (factR < factN_R ? (factN / factN_R / factR) : (factN / factR / factN_R));
  }
}