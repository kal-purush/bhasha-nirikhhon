package com.github.lucaseo90.algo.days21.day10;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CombinationSum {
    List<List<Integer>> combinations = new ArrayList<>();

    public List<List<Integer>> combinationSum(int[] candidates, int target) {
        int n = candidates.length;
        Arrays.sort(candidates);
        findCombination(candidates, new ArrayList<>(), n, target, 0, 0);
        return combinations;
    }

    private void findCombination(int[] candidates, ArrayList<Integer> elements, int n, int target, int sum, int depth) {
        if (target == sum) {
            combinations.add(new ArrayList<>(elements));
            return;
        }

        for (int i = depth; i < n; i++) {
            if (sum + candidates[i] <= target) {
                elements.add(candidates[i]);
                findCombination(candidates, elements, n, target, sum + candidates[i], depth++);
                elements.remove(elements.size() - 1);
            }
        }
    }
}