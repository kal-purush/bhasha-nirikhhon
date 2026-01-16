package rish.leets.contests.weekly;

import java.util.TreeMap;
import java.util.TreeSet;

/*
 *        Weekly Contest 352
 * 
 * Problem #: 2762
 * Problem link: https://leetcode.com/problems/continuous-subarrays
 * 
 * Attempt Date: 18/07/2023
 * 
 * @author: Rishabh Soni
 * 
 */
public class ContinousSubArrays {

    /*
     * Two pointer method (optimised)
     * 
     * Time complexity: O(n)
     * 
     */
    public static long continuousSubarrays(int[] nums) {

        long ans = 0;
        TreeMap<Integer, Integer> map = new TreeMap<>();

        for (int m = 0, n = 0; n < nums.length; n++) {

            int freq = map.getOrDefault(nums[n], 0);
            map.put(nums[n], freq + 1);

            while (!map.isEmpty() && map.lastKey() - map.firstKey() > 2) {

                map.put(nums[m], map.getOrDefault(nums[m], 1) - 1);

                if (map.get(nums[m]) == 0) {
                    map.remove(nums[m]);
                }

                m++;
            }

            // Add size of sub-array, since all of them are continuous
            ans += n - m + 1;
        }

        return ans;
    }

    /*
     * Brute force approach (non-optimized)
     * 
     * Time complexity : O(n^2)
     * 
     */
    public static long continuousSubarrays2(int[] nums) {

        long ans = 0;

        for (int i = 0; i < nums.length; i++) {

            TreeSet<Integer> set = new TreeSet<>();
            set.add(nums[i]);

            for (int j = i; j < nums.length; j++) {

                set.add(nums[j]);
                int min = set.first();
                int max = set.last();

                if (Math.abs(max - min) > 2) {
                    break;
                }

                ans++;
            }
        }

        return ans;
    }

    public static void main(String[] args) {
        int[] arr = { 42, 41, 42, 41, 41, 40, 39, 38 };
        System.out.println(continuousSubarrays(arr));
    }

}