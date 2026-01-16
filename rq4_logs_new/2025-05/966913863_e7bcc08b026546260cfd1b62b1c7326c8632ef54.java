package leetcode;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class ThreeSumClosest {

    private static void calculateClosestSum(int[] arr, int target) {
        HashSet<List<Integer>> result = new HashSet<>();
        for (int i = 0; i < arr.length - 2; i++) {
            int left = i + 1;
            int right = arr.length - 1;
            int sum = arr[i] + arr[left] + arr[right];
            if (sum == target) {
                result.add(Arrays.asList(arr[i], arr[left], arr[right]));
                left++;
                right--;
            } else if (sum > target) {
                right--;
            } else {
                left++;
            }
        }
        System.out.println(result);
    }

    public static void main(String[] args) {
        int[] arr = {-4, -1, 0, 1, 2};
        int target = 1;
        calculateClosestSum(arr, target);
    }
}