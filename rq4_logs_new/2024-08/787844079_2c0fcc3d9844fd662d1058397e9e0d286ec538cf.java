//https://leetcode.com/problems/max-consecutive-ones/
package algorithms.easy;

public class MaxConsecutiveOnes {
    public static void main(String[] args) {
        MaxConsecutiveOnes maxOnes = new MaxConsecutiveOnes();
        System.out.println("Output:\t" + maxOnes.findMaxConsecutiveOnes(new int[]{1, 1, 0, 1, 1, 1}));
        System.out.println("Output:\t" + maxOnes.findMaxConsecutiveOnes(new int[]{1, 0, 1, 1, 0, 1}));
    }

    public int findMaxConsecutiveOnes(int[] nums) {
        int max = Integer.MIN_VALUE;
        int current = 0;

        for (int num : nums) {
            if (num == 1) current++;
            else {
                max = Math.max(max, current);
                current = 0;
            }
        }

        max = Math.max(max, current);
        return max;
    }
}