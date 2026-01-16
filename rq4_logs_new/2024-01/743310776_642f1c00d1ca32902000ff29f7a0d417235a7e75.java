package com.vmelnyk.leetcode._746MinCostClimbingStairs;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SolutionTest {

  @Test
  void minCostClimbingStairs() {
    Solution solution = new Solution();

    assertEquals(15, solution.minCostClimbingStairs(new int[] {10, 15, 20}));
    System.out.println();
    assertEquals(6, solution.minCostClimbingStairs(new int[] {1, 100, 1, 1, 1, 100, 1, 1, 100, 1}));
    System.out.println();
    assertEquals(2, solution.minCostClimbingStairs(new int[] {0, 1, 2, 2}));
  }
}