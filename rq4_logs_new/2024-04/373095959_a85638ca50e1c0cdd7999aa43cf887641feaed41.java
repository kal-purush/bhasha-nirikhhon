package org.frank.leetcode.array.first.missing.positive.demo03;

import java.util.HashSet;
import java.util.Set;

public class FirstMissingPositiveSolution3 {

    public int firstMissingPositive(int[] nums) {
        Set<Integer> found = new HashSet<>();

        // Add only positive numbers to the set
        for (int number : nums) {
            if (number > 0) {
                found.add(number);
            }
        }

        // Start checking from 1 to see which is the first number not in the set
        int smallestMissingPositive = 1;
        while (found.contains(smallestMissingPositive)) {
            smallestMissingPositive++;
        }

        return smallestMissingPositive;
    }
}