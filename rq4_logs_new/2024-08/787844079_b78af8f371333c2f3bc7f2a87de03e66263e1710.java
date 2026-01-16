//https://leetcode.com/problems/array-partition/
package algorithms.easy;

import java.util.Arrays;

public class ArrayPartition {
    public static void main(String[] args) {
        ArrayPartition arrayPartition = new ArrayPartition();
        System.out.println("Output:\t" + arrayPartition.arrayPairSum(new int[]{1, 4, 3, 2}));
        System.out.println("Output:\t" + arrayPartition.arrayPairSum(new int[]{6, 2, 6, 5, 1, 2}));
    }

    public int arrayPairSum(int[] nums) {
        Arrays.sort(nums);
        int answer = 0;
        for (int i = 0; i < nums.length; i += 2)
            answer += nums[i];
        return answer;
    }
}