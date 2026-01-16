package com.github.lucaseo90.algo.days21.day9;

import java.util.ArrayList;
import java.util.List;

public class SubSets {
    private final List<List<Integer>> candidateSets = new ArrayList<>();

    public List<List<Integer>> subsets(int[] nums) {
        findSubSet(nums, new ArrayList<>(), 0);
        return new ArrayList<>(candidateSets);
    }

    private void findSubSet(int[] nums, ArrayList<Integer> elements, int depth) {
        if (depth == nums.length) {
            candidateSets.add(new ArrayList<Integer>(elements));
            return;
        }

        elements.add(nums[depth]);
        findSubSet(nums, elements, depth + 1);
        elements.remove(elements.size() - 1);
        findSubSet(nums, elements, depth + 1);
    }
}