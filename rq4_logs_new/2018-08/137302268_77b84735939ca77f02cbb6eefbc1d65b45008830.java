import java.util.HashMap;
import java.util.Map;

/**
 * @author Patrick-lenovo
 * @create 2018/6/14 14:17
 **/
class twoSum {

    public int[] twoSum(int[] nums, int target) {
        // value : key
        Map<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < nums.length; ++i) {
            if (map.containsKey(target - nums[i])) {
                return new int[]{map.get(target - nums[i]), i};
            } else {
                map.put(nums[i], i);
            }
        }

        throw new IllegalArgumentException("No two sum solution.");
    }
}