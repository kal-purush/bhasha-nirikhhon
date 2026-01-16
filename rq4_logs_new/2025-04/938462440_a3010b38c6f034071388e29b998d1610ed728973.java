package dsaQuetions.medium;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MajorityElementII {

    //  Time complexity: O(n^2)
    //  Space complexity:
    //          Extra: O(n)
    //          Algo: O(n)
    //  Pattern: Brute force
    public static List<Integer> getMajorityElementsBruteForce(int[] nums) {
        int len = nums.length;
        List<Integer> majorityElements = new ArrayList<Integer>();

        for (int i = 0; i < len; i++) {
            int count = 0;

            for (int j = i; j < len; j++) {
                if (nums[i] == nums[j]) count++;
            }

            if (count > len / 3 && !majorityElements.contains(nums[i])) {
                majorityElements.add(nums[i]);
            }
        }
        return majorityElements;
    }

    //  Time complexity: O(n)
    //  Space complexity:
    //          Extra: O(n)
    //          Algo: O(n)
    //  Pattern: Hashing.
    public static List<Integer> majorityElementBetterApproach(int[] nums) {
        Map<Integer, Integer> countMap = new HashMap<>();
        List<Integer> majorityElements = new ArrayList<>();
        int threshold = nums.length / 3;
        // Count occurrences
        for (int num : nums) {
            countMap.put(num, countMap.getOrDefault(num, 0) + 1);
        }
        // Check for majority elements
        for (Map.Entry<Integer, Integer> entry : countMap.entrySet()) {
            if (entry.getValue() > threshold) {
                majorityElements.add(entry.getKey());
            }
        }
        return majorityElements;
    }
}