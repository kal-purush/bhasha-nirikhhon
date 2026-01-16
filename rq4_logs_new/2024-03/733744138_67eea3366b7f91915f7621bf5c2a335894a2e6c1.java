package br.dev.ferreiras.challenges.leetCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
Given an integer array nums of length n where all the integers of nums are in the
range [1, n] and each integer appears once or twice, return an array of all the
integers that appears twice.

You must write an algorithm that runs in O(n) time and uses only constant extra space.

Example 1:

Input: nums = [4,3,2,7,8,2,3,1]
Output: [2,3]
Example 2:

Input: nums = [1,1,2]
Output: [1]
Example 3:

Input: nums = [1]
Output: []


Constraints:

n == nums.length
1 <= n <= 105
1 <= nums[i] <= n
Each element in nums appears once or twice.

Intuition
To solve this problem in O(n) time and constant extra space, we can utilize the fact
that all integers in the array are in the range [1, n] and each integer appears once or twice.
We can iterate through the array and use each element as an index to mark the corresponding
element as negative. If we encounter an element that is already negative,
it means we have seen it before and it's a duplicate.

Approach
Initialize an empty vector list to store the duplicates.
Iterate through the array nums.
For each element nums[i], take its absolute value num.
Check if nums[num-1] is negative.
If it is, then num is a duplicate, so add it to list.
Otherwise, mark nums[x-1] as negative.
Return list.
Complexity
Time complexity:O(n)
Space complexity:O(k)
max value of k = n/2
 */
public class FindDuplicatesOne {
  public static void main(String[] args) {
    int[] nums = {4, 3, 2, 7, 8, 2, 3, 1};
    List<Integer> list = duplicates(nums);
    System.out.println(list);
  }

  public static List<Integer> findDuplicates(int[] nums) {
    Map<Integer, Integer> map = new HashMap<>();
    List<Integer> list = new ArrayList<>();
    for (int num : nums) {
      map.put(num, map.getOrDefault((num), 0) + 1);
    }

    for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
      if (entry.getValue() == 2) {
        list.add(entry.getKey());
      }
    }

    return list;
  }

  public static List<Integer> duplicates(int[] nums) {
    List<Integer> list = new ArrayList<>();
    int size = nums.length;

    for (int i = 0; i < size; i++) {

      int num = Math.abs(nums[i]);

      if (nums[num - 1] < 0) {
        list.add(num);
      }

      nums[num - 1] *= -1;
    }

    return list;
  }
}
