package rish.leets.contests.biweekly;

import java.util.HashMap;
import java.util.Map;

/*
 *        BiWeekly Contest 109
 * 
 * Problem #: 2784
 * Contest link: https://leetcode.com/contest/biweekly-contest-109/
 * Problem link: https://leetcode.com/problems/check-if-array-is-good/
 * 
 * Attempt Date: 22/07/2023
 * 
 * @author: Rishabh Soni
 *  
 */
public class GoodPermutedArray {

    public boolean isGood(int[] nums) {

        Map<Integer, Integer> map = new HashMap<>();

        int max = 0;
        for (int x : nums) {
            max = Math.max(max, x);
            int val = map.getOrDefault(x, 0);
            map.put(x, val + 1);
        }

        if (max + 1 != nums.length) {
            return false;
        }

        if (map.get(max) != 2) {
            return false;
        }

        for (int i = 1; i <= max; i++) {
            if (!map.containsKey(i)) {
                return false;
            }
        }

        return true;
    }

}