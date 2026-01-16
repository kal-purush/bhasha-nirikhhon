package algorithms.array;

import java.util.LinkedList;

/***
 * You are given a 0-indexed integer array nums and an integer k.

    You are initially standing at index 0. In one move, you can jump at most k steps forward without going outside the boundaries of the array. That is, you can jump from index i to any index in the range [i + 1, min(n - 1, i + k)] inclusive.
    
    You want to reach the last index of the array (index n - 1). Your score is the sum of all nums[j] for each index j you visited in the array.
    
    Return the maximum score you can get.
    
     
    
    Example 1:
    
    Input: nums = [1,-1,-2,4,-7,3], k = 2
    Output: 7
    Explanation: You can choose your jumps forming the subsequence [1,-1,4,3] (underlined above). The sum is 7.
    Example 2:
    
    Input: nums = [10,-5,-2,4,0,3], k = 3
    Output: 17
    Explanation: You can choose your jumps forming the subsequence [10,4,3] (underlined above). The sum is 17.
    Example 3:
    
    Input: nums = [1,-5,-20,4,-1,3,-6,-3], k = 2
    Output: 0
     
    
    Constraints:
    
    1 <= nums.length, k <= 10^5
    -10^4 <= nums[i] <= 10^4
 * @author michel
 *
 */
public class JumpGameVI {

    /***
     * Let dp[i] be "the maximum score to reach the end starting at index i". 
     * The answer for dp[i] is nums[i] + max{dp[i+j]} for 1 <= j <= k.
     * Instead of checking every j for every i, keep track of the largest dp[i] values in a heap and calculate dp[i] from right to left. 
     * When the largest value in the heap is out of bounds of the current index, remove it and keep checking.
     * @param nums
     * @param k
     * @return
     */
    public int maxResult(int[] nums, int k) {
        int n = nums.length;
        int[] dp = new int[n];
        LinkedList<Integer> queue = new LinkedList<Integer>();
        dp[n-1] = nums[n-1];
        queue.offer(n-1);
        for(int i = n-2; i >= 0; i--) {
            if(queue.peek() > i+k) {
                queue.poll();
            }
            dp[i] = nums[i] + dp[queue.peek()];
            while(!queue.isEmpty() && dp[queue.getLast()] <= dp[i]) queue.pollLast();
            queue.offer(i);
        }
        return dp[0];
    }

}