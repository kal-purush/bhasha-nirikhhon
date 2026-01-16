//https://leetcode.com/problems/kth-distinct-string-in-an-array/
package algorithms.easy;

import algorithms.medium.EditDistance;

import java.util.HashMap;

public class KthDistinctStringInAnArray {
    public static void main(String[] args) {
        KthDistinctStringInAnArray distinct = new KthDistinctStringInAnArray();
        System.out.println("Output:\t" + distinct.kthDistinct(new String[]{"d", "b", "c", "b", "c", "a"}, 2));
        System.out.println("Output:\t" + distinct.kthDistinct(new String[]{"aaa", "aa", "a"}, 1));
        System.out.println("Output:\t" + distinct.kthDistinct(new String[]{"a", "b", "a"}, 3));
    }

    public String kthDistinct(String[] arr, int k) {
        HashMap<String, Integer> map = new HashMap<>();

        for (String s : arr)
            map.put(s, map.getOrDefault(s, 0) + 1);

        for (String s : arr) {
            if (map.get(s) == 1)
                k--;
            if (k == 0)
                return s;
        }
        return "";
    }
}