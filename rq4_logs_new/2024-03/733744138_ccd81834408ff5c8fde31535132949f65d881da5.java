package br.dev.ferreiras.challenges.leetCode;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/*
You are given an array nums consisting of positive integers.
Return the total frequencies of elements in nums such that those elements
all have the maximum frequency.

The frequency of an element is the number of occurrences of that element in the array.
Example 1:
Input: nums = [1,2,2,3,1,4]
Output: 4
Explanation: The elements 1 and 2 have a frequency of 2 which is the maximum frequency in the array.
So the number of elements in the array with maximum frequency is 4.
Example 2:

Input: nums = [1,2,3,4,5]
Output: 5
Explanation: All elements of the array have a frequency of 1 which is the maximum.
So the number of elements in the array with maximum frequency is 5.
 */
public class MaximumFrequency {
  public static void main(String[] args) {
    int[] nums = {1,2,2,3,1,4};
    int sum = MaximumFrequency.maxFrequencyElements(nums);
    System.out.println(sum);
  }

  public static int maxFrequencyElements(int[] nums) {
    Map<Integer, Integer> frequency = new HashMap<>();

    for (int num : nums) {

      if (!frequency.containsKey(num)) {
        frequency.put(num, 1);
      } else {
        frequency.put(num, frequency.getOrDefault((num), 1) + 1);
      }

    }

    int countMax = 0;
    int maxValue = Collections.max(frequency.values());

    for (Map.Entry<Integer, Integer> entry : frequency.entrySet()) {

      if (entry.getValue() == maxValue) {
        countMax++;
      }

    }
    System.out.println(maxValue);

    return maxValue*countMax;
  }
}