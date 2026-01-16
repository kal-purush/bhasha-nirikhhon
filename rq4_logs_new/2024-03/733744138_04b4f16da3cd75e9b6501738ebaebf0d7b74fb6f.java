package br.dev.ferreiras.challenges.leetCode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
Given an array of integers nums sorted in non-decreasing order,
find the starting and ending position of a given target value.
If target is not found in the array, return [-1, -1].
You must write an algorithm with O(log n) runtime complexity.
Example 1:
Input: nums = [5,7,7,8,8,10], target = 8
Output: [3,4]
Example 2:
Input: nums = [5,7,7,8,8,10], target = 6
Output: [-1,-1]
Example 3:
Input: nums = [], target = 0
Output: [-1,-1]

 */
public class FirstLastPositionSortedArray {
  public static void main(String[] args) {
    int[] numbers = {5,7,7,8,8,10};
    int target = 8;
    int[] response = searchRange(numbers,target);
    System.out.println(Arrays.toString(response));
  }
  public static int[] searchRange(int[] nums, int target) {
    int[] response = {-1,-1};
    int start = helperSearch(nums, target, true);
    int end =   helperSearch(nums,target,false);
    response[0] = start;
    response[1] = end;

    return response;
  }
  public static int helperSearch(int[] nums, int target, boolean flag) {
    int response  = -1;
    int size = nums.length;
    int left = 0, middle = 0, right = 0;
    right = size - 1;
    while (left <= right) {
      middle = (int) (left + (right - left) / 2);
      System.out.println(middle);
      if (target < nums[middle]) {
        right = middle - 1;
      } else if (target > nums[middle]) {
        left = middle + 1;
      } else {
        response = middle;
        if (flag) {
          right = middle - 1;
        } else {
          left = middle + 1;
        }
      }
    }

    return response;
  }
}