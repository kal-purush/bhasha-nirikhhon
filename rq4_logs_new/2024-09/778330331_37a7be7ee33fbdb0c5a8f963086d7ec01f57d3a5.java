package Greedy;

public class JumpGame_2 {
    class Solution {
        public int jump(int[] nums) {
          // Initialize the number of jumps taken, the current range we are exploring (currEnd), 
          // and the farthest index we can reach so far (currFarthest)
          int jumps = 0, currEnd = 0, currFarthest = 0;
    
          // Iterate through the array up to the second last element (nums.length - 1),
          // as we don’t need to check the last element since we are jumping towards it
          for(int i = 0; i < nums.length - 1; i++) {
              // Update the farthest point that can be reached from the current index
              currFarthest = Math.max(currFarthest, i + nums[i]);
    
              // When we reach the current "end of range" for this jump (currEnd),
              // it means we need to make a new jump
              if(i == currEnd) {
                  // Move the current range (currEnd) to the farthest point we can reach (currFarthest)
                  currEnd = currFarthest;
    
                  // Increment the jump count as we need to jump again to explore further
                  jumps++;
              }
          }
    
          // Return the total number of jumps needed to reach the last index
          return jumps;
        }
    }
    
}