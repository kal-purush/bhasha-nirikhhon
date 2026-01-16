//https://leetcode.com/problems/make-two-arrays-equal-by-reversing-subarrays/
package algorithms.easy;

import java.util.Arrays;
import java.util.HashMap;

public class MakeTwoArraysEqualByReversingSubarrays {
    public static void main(String[] args) {
        MakeTwoArraysEqualByReversingSubarrays equal = new MakeTwoArraysEqualByReversingSubarrays();
        System.out.println("Output:\t" + equal.canBeEqualViaCounting(new int[]{1, 2, 3, 4}, new int[]{2, 4, 1, 3}));
        System.out.println("Output:\t" + equal.canBeEqualViaCountingOptimized(new int[]{7}, new int[]{7}));
        System.out.println("Output:\t" + equal.canBeEqualOptimized(new int[]{3, 7, 9}, new int[]{3, 7, 11}));
    }

    public boolean canBeEqualViaCounting(int[] target, int[] arr) {
        HashMap<Integer, Integer> map = new HashMap<>();

        for (int num : target)
            map.put(num, map.getOrDefault(num, 0) + 1);

        for (int num : arr)
            if (!map.containsKey(num) || map.get(num) < 1)
                return false;
            else
                map.put(num, map.getOrDefault(num, 0) - 1);

        return true;
    }

    public boolean canBeEqualViaCountingOptimized(int[] a, int[] b) {
        HashMap<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < a.length; i++) {
            map.put(a[i], map.getOrDefault(a[i], 0) + 1);
            map.put(b[i], map.getOrDefault(b[i], 0) - 1);
        }
        for (int i : map.keySet()) {
            if (map.get(i) != 0)
                return false;
        }
        return true;
    }

    public boolean canBeEqualOptimized(int[] target, int[] arr) {
        var map = new int[1001];
        for (var n : arr)
            map[n]++;
        for (var n : target)
            map[n]--;
        return Arrays.equals(map, new int[1001]);
    }

}