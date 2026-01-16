package Array;

import java.util.HashMap;
import java.util.Map;

public class SubarraySumEqualsK {
    public int subarraySum(int[] nums, int k) {
        int count = 0, currSum = 0;
        Map<Integer, Integer> map = new HashMap<>();

        for (int num : nums) {
            // current prefix sum
            currSum += num;

            // situation 1:
            // continuous subarray starts from the beginning of the array
            if (currSum == k) {
                count++;
            }

            // situation 2:
            // number of times the currSum - k has occurred already,
            // determines the number of times a subarray with sum k has occurred upto the current index
            count += map.getOrDefault(currSum - k, 0);

            // add the current sum
            map.put(currSum, map.getOrDefault(currSum, 0) + 1);
        }

        return count;
    }
}