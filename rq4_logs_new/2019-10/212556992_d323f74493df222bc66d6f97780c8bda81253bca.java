/**
 * 全排列，去重
 * 递归
 */

import java.util.*;

class Solution {
    public List<List<Integer>> permuteUnique(int[] nums) {
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
            if (nums[0] == nums[1]) {
                List<List<Integer>> temp = new ArrayList<>();
                temp.add(Arrays.asList(nums[0], nums[1]));
                return temp;
            }
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

            List<List<Integer>> re = permuteUnique(arr);

            for (List<Integer> r : re) {
                List<Integer> a = new ArrayList<>();
                a.add(nums[i]);
                a.addAll(r);
                result.add(a);
            }
        }

        Set<List<Integer>> set = new HashSet<>();
        for (List<Integer> r : result) {
            set.add(r);
        }
        List<List<Integer>> fromSet = new ArrayList<>();
        for (List<Integer> s : set) {
            fromSet.add(s);
        }
        return fromSet;
    }
}

class Test {
    public static void main(String[] args) {
        Solution s = new Solution();
        int[] nums = {1, 1, 3};
        System.out.println(s.permuteUnique(nums));
    }
}
