package com.github.lucaseo90.algo.days21.day9;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SubSetsII {
    private final Set<List<Integer>> candidateSets = new HashSet<>();

    public List<List<Integer>> subsetsWithDup(int[] nums) {
        Arrays.sort(nums);
        findSubSet(nums, new ArrayList<Integer>(), 0);
        return new ArrayList<>(candidateSets);
    }

    private void findSubSet(int[] nums, ArrayList<Integer> elements, int depth) {
        if (depth == nums.length) {
            Collections.sort(elements);
            candidateSets.add(new ArrayList<>(elements));
            return;
        }

        elements.add(nums[depth]);
        findSubSet(nums, elements, depth + 1);
        elements.remove(elements.size() - 1);
        findSubSet(nums, elements, depth + 1);
    }
}