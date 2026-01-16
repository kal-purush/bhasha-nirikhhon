//https://leetcode.com/problems/longest-harmonious-subsequence/
package algorithms.easy;

import java.util.HashMap;
import java.util.Map;

public class LongestHarmoniousSubsequence {
    public static void main(String[] args) {
        LongestHarmoniousSubsequence subsequence = new LongestHarmoniousSubsequence();
        System.out.println("Output:\t" + subsequence.findLHS(new int[]{1, 3, 2, 2, 5, 2, 3, 7}));
        System.out.println("Output:\t" + subsequence.findLHS(new int[]{1, 2, 3, 4}));
        System.out.println("Output:\t" + subsequence.findLHS(new int[]{1, 1, 1, 1}));
    }

    public int findLHS(int[] nums) {
        Map<Integer, Integer> map = new HashMap<>();
        int harmonious = 0;
        for (int num : nums)
            map.put(num, map.getOrDefault(num, 0) + 1);

        for (int num : nums)
            if (map.containsKey(num + 1)) harmonious = Math.max(harmonious, map.get(num) + map.get(num + 1));

        return harmonious;
    }
}