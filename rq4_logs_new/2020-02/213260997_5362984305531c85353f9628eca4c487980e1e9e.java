package LeetCode;

import java.util.HashSet;
import java.util.Set;

public class LC128_Longest_consecutive_Subsequence {
    public int longestConsecutive(int[] num) {
        if (num == null || num.length == 0) return 0;
        Set<Integer> set = new HashSet<>();
        int res = 0;
        for (int n : num) {
            set.add(n);
        }

        for (int n : num) {
            if (set.contains(n)) {
                set.remove(n);
            }

            int small = n - 1;
            int large = n + 1;

            while (set.contains(small)) {
                set.remove(small);
                small--;
            }

            while (set.contains(large)) {
                set.remove(large);
                large++;
            }
            res = Math.max(res, large - small - 1);
        }

        return res;
    }
}