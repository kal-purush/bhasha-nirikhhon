package leetcode;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class FourSum {
    public static void main(String[] args) {
        int[] nums = {1, 0, -1, 0, -2, 4};
        int target = 0;
        fourSumSolution(nums, target);
    }

    private static void fourSumSolution(int[] nums, int target) {
        Arrays.sort(nums);
        HashSet<List<Integer>> result = new HashSet<>();
        for (int i = 0; i < nums.length - 3; i++) {
            for (int j = i + 1; j < nums.length - 1; j++) {
                int left = j + 1;
                int right = nums.length - 1;

                while (left < right) {
                    int sum = nums[i] + nums[j] + nums[left] + nums[right];
//                    System.out.println("Sum : "+sum);
                    result.add(Arrays.asList(nums[i],nums[j],nums[left],nums[right]));
                    left++;
                    right--;
//                    if (sum == target) {
//                        result.add(Arrays.asList(nums[i], nums[j], nums[left], nums[right]));
//                    } else if (sum < target) {
//                        left++;
//                    } else {
//                        right++;
//                    }
                }
            }
        }
        System.out.println(result);
    }
}