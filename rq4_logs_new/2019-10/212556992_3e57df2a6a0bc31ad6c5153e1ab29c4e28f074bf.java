/**
 * 全排列，不去重
 * 递归
 */

import java.util.ArrayList;
import java.util.List;

class Solution {
    public List<List<Integer>> permute(int[] nums) {
        List<List<Integer>> result = new ArrayList<>();

        if (nums.length == 0) {
            return null;
        }
        else if (nums.length == 1) {
            List<Integer> l = new ArrayList<>();
            l.add(nums[0]);
            result.add(l);
            return result;
        }
        else if (nums.length == 2) {
            List<Integer> l = new ArrayList<>();
            l.add(nums[0]);
            l.add(nums[1]);
            List<Integer> l1 = new ArrayList<>();
            l1.add(nums[1]);
            l1.add(nums[0]);
            result.add(l);
            result.add(l1);
            return result;
        }

        for (int i = 0; i < nums.length; ++i) {
            int[] arr = new int[nums.length - 1];
            boolean f = false;
            for (int j = 0; j < nums.length - 1; ++j) {
                if (j < i) {
                    arr[j] = nums[j];
                }
                else if (j == i && !f) {
                    --j;
                    f = true;
                    continue;
                }
                else {
                    arr[j] = nums[j + 1];
                }
            }

            List<List<Integer>> re = permute(arr);

            for (List<Integer> r : re) {
                List<Integer> a = new ArrayList<>();
                a.add(nums[i]);
                a.addAll(r);
                result.add(a);
            }
        }

        return result;
    }
}

class Test {
    public static void main(String[] args) {
        Solution s = new Solution();
        int[] nums = {1, 2, 3};
        System.out.println(s.permute(nums));
    }
}
