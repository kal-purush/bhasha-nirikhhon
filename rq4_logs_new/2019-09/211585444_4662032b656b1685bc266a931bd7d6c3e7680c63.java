package com.zzm;

import java.util.ArrayList;
import java.util.List;

/**
 给定一个没有重复数字的序列，返回其所有可能的全排列。

 示例:

 输入: [1,2,3]
 输出:
 [
 [1,2,3],
 [1,3,2],
 [2,1,3],
 [2,3,1],
 [3,1,2],
 [3,2,1]
 ]

 来源：力扣（LeetCode）
 链接：https://leetcode-cn.com/problems/permutations
 */
public class Algorithm {
    public static void main(String[] args) {

        int[]  nums = {1,2,3};
        List<List<Integer>> res = premute(nums) ;
        for (int i = 0 ; i < res.size();i++){
            System.out.println(res.get(i));
        }

    }

    public static List<List<Integer>> premute(int[] nums){
        List<List<Integer>> res = new ArrayList();
        getPermutations(nums,res,new ArrayList(),new ArrayList());
        return res;
    }


    public static void getPermutations(int[] nums,List<List<Integer>> res ,List<Integer> current,List<Integer> coveredIndexs){

        if (current.size() == nums.length){
            res.add(current);
        }else {
            for (int i = 0; i <nums.length ; i++){
                if (!coveredIndexs.contains(i)){
                    List<Integer> temp = new ArrayList(current);
                    temp.add(nums[i]);
                    List<Integer> tempCoveredIndices = new ArrayList(coveredIndexs);
                    tempCoveredIndices.add(i);
                    getPermutations(nums,res,temp,tempCoveredIndices);
                }
            }
        }
    }

}