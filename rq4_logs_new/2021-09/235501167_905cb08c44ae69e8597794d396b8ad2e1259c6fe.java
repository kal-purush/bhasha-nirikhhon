package dp;

/**
 * https://leetcode.com/problems/min-cost-climbing-stairs/
 *
 * You are given an integer array cost where cost[i] is the cost of ith step on a staircase. Once you pay the cost, you can either climb one or two steps.
 *
 * You can either start from the step with index 0, or the step with index 1.
 *
 * Return the minimum cost to reach the top of the floor.
 */
public class Min_Cost_Climbing_Stairs {
    class Solution {
        public int minCostClimbingStairs(int[] cost) {
            if(cost.length == 2) return Math.min(cost[0], cost[1]);

            int[] steps = new int[cost.length];

            steps[0] = cost[0];
            steps[1] = cost[1];

            for(int i = 2; i < cost.length; i++) {
                steps[i] = Math.min(steps[i-1], steps[i-2]) + cost[i];
            }

            return Math.min(steps[cost.length-1], steps[cost.length-2]);
        }
    }
}