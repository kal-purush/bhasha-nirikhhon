package com.github.lucaseo90.algo.days21.day3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ThreeSum {
    private final Set<List<Integer>> threeSums = new HashSet<>();

    public List<List<Integer>> threeSum(int[] nums) {
        boolean[] visited = new boolean[nums.length];
        Arrays.sort(nums);
        findThreeSum(nums, visited, new ArrayList<>(), 3, 0, 0);
        return new ArrayList<>(threeSums);
    }

    public void findThreeSum(int[] nums, boolean[] visited, ArrayList<Integer> elements, int r, int sum, int depth) {
        if (elements.size() == r && sum == 0) {
            threeSums.add(new ArrayList<>(elements));
            return;
        }

        for (int i = depth; i < nums.length; i++) {
            if (!visited[i] && sum <= 0) {
                visited[i] = true;
                elements.add(nums[i]);
                findThreeSum(nums, visited, elements, r, sum + nums[i], i);
                elements.remove(elements.size() - 1);
                visited[i] = false;
            }
        }
    }
}