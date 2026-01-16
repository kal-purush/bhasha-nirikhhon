package mytest.algorithm.slidingwindow;

import java.util.*;

/**
 * 给定一个包含 n 个整数的数组 nums 和一个目标值 target，判断 nums 中是否存在四个元素 a，b，c 和 d ，使得 a + b + c +
 * // d 的值与 target 相等？找出所有满足条件且不重复的四元组。
 * //
 * // 注意：
 * //
 * // 答案中不可以包含重复的四元组。
 * //
 * // 示例：
 * //
 * // 给定数组 nums = [1, 0, -1, 0, -2, 2]，和 target = 0。
 * //
 * //满足要求的四元组集合为：
 * //[
 * //  [-1,  0, 0, 1],
 * //  [-2, -1, 1, 2],
 * //  [-2,  0, 0, 2]
 * //]
 * //
 * // Related Topics 数组 哈希表 双指针
 *
 * https://leetcode-cn.com/problems/4sum/
 * @author : yufei
 * @date : 2021/1/14 19:19
 * @description :
 */
public class FourSum {

    boolean[] used;

    public static void main(String[] args) {
        System.out.println(new FourSum().fourSum(new int[]{-493,-482,-482,-456,-427,-405,-392,-385,-351,-269,-259,-251,-235,-235,-202,-201,-194,-189,-187,-186,-180,-177,-175,-156,-150,-147,-140,-122,-112,-112,-105,-98,-49,-38,-35,-34,-18,20,52,53,57,76,124,126,128,132,142,147,157,180,207,227,274,296,311,334,336,337,339,349,354,363,372,378,383,413,431,471,474,481,492},6189));
        System.out.println("re");
    }
    // 四重循环，双指针，先固定前面两个数，然后另外遍历剩余的两个数，先排序，遇到重复的跳过
    public List<List<Integer>> fourSum(int[] nums, int target) {
        if (nums == null || nums.length<4){
            return Collections.emptyList();
        }
        List<List<Integer>> result = new ArrayList<>();
        Arrays.sort(nums);

        int left = 0;
        int preLeft = 0;
        while(left < nums.length-3){
            if (left>0 && nums[left]==preLeft){
                left++; // 这里加上，不然会死循环
                continue;
            }
            int preRight=0;
            int initIndex = left +1;
            for (int right = left +1; right<nums.length-2; right++){
                if (right>initIndex && nums[right] == preRight){
                    continue;
                }
                int preILeft = 0;
                int indexIL = right+1;
                for (int iLeft = right+1; iLeft<nums.length-1; iLeft++){
                    if (iLeft>indexIL && nums[iLeft] == preILeft){
                        continue;
                    }
                    int preIRight = 0;
                    int indexIR = iLeft+1;
                    for (int iRight = iLeft +1;iRight<nums.length;iRight++){
                        if (iRight>indexIR && nums[iRight] == preIRight){
                            continue;
                        }
                        if ((nums[left]+nums[right]+nums[iLeft]+nums[iRight]) == target){
                            List<Integer> line = new ArrayList<>();
                            line.add(nums[left]);
                            line.add(nums[right]);
                            line.add(nums[iLeft]);
                            line.add(nums[iRight]);
                            result.add(line);
                        }
                        preIRight = nums[iRight];
                    }
                    preILeft = nums[iLeft];
                }
                preRight=nums[right];
            }
            preLeft = nums[left];
            left++;
        }
        return result;
    }

    // 使用backtrack 回溯算法,太慢
    public List<List<Integer>> fourSum2(int[] nums, int target) {
        if (nums == null || nums.length<4){
            return Collections.emptyList();
        }
        Arrays.sort(nums);
        used = new boolean[nums.length];
        List<List<Integer>> result = new ArrayList<>();
        LinkedList<Integer> track = new LinkedList<>();
        backtrack(nums,result,0,target,track);

        return result;
    }

    private void backtrack(int[] nums,  List<List<Integer>> result, int start,int target, LinkedList<Integer> track){
        if (track.size() == 4 &&  track.stream().mapToInt(i->i).sum() == target){
            result.add(new ArrayList<>(track));
            return;
        }

        Integer pre = null;
        for (int i =start; i<nums.length; i++){
            if (used[i] || (pre!=null && nums[i] == pre)){
                continue;
            }
            track.add(nums[i]);
            used[i] = true;
            backtrack(nums,result,i+1,target,track);
            used[i] =false;
            track.removeLast();
            pre = nums[i];
        }
    }
}