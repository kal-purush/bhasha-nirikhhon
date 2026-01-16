package com.github.lucaseo90.algo.days21.day10;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PermutationsII {
    private final Set<List<Integer>> sets = new HashSet<>();

    public List<List<Integer>> permuteUnique(int[] nums) {
        int n = nums.length;
        boolean[] visited = new boolean[n];
        findPermutation(nums, visited, new ArrayList<>(), n);
        return new ArrayList<>(sets);
    }

    private void findPermutation(int[] nums, boolean[] visited, ArrayList<Integer> elements, int n) {
        if (elements.size() == n) {
            sets.add(new ArrayList<>(elements));
            return;
        }

        for (int i = 0; i < n; i++) {
            if (!visited[i]) {
                visited[i] = true;
                elements.add(nums[i]);
                findPermutation(nums, visited, elements, n);
                elements.remove(elements.size() - 1);
                visited[i] = false;
            }
        }
    }
}