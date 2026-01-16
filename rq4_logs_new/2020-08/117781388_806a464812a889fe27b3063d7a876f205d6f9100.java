package DepthFirstSearch.Backtracking;

import java.util.ArrayList;
import java.util.List;

public class Combinations {
    public List<List<Integer>> combine(int n, int k) {
        List<List<Integer>> result = new ArrayList<>();
        List<Integer> combination = new ArrayList<>();

        dfs(result, combination, n, k, 1);
        return result;
    }

    public void dfs(List<List<Integer>> result, List<Integer> combination, int n, int k, int start) {
        if (combination.size() == k) {
            result.add(new ArrayList<>(combination));
        }

        for (int i = start; i <= n; i++) {
            combination.add(i);
            dfs(result, combination, n, k, i + 1);
            combination.remove(combination.size() - 1);
        }
    }
}