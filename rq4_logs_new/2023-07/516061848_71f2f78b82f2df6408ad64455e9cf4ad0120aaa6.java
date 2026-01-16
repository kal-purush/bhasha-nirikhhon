package Easy._888_Fair_Candy_Swap;

import java.util.HashSet;
import java.util.Set;

public class Solution {

    public int[] fairCandySwap(int[] aliceSizes, int[] bobSizes) {
        int[] result = new int[2];
        int total_A = 0;
        int total_B = 0;

        Set<Integer> setB = new HashSet<>();

        for (int a : aliceSizes) {
            total_A += a;
        }

        for (int b : bobSizes) {
            total_B += b;
            setB.add(b);
        }

        int delta = (total_B - total_A) / 2;

        for (int a : aliceSizes) {
            if (setB.contains(a + delta)) {
                result[0] = a;
                result[1] = a + delta;
                return result;
            }
        }

        return null;

    }

}