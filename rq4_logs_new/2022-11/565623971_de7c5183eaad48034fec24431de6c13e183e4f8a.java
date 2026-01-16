package com.yearup.week4.code.complexity;

/*Given an array of integers, return two numbers such that sum
is equal to a given target.*/

import java.io.*;
import java.util.*;

public class TwoNumberSum {
    //Time complexity: O(n^2) Brute force approach
    public static int[] targetTwoNumberSumOn2(int[] arr, int target) {
        for (int i = 0; i < arr.length; i++) {
            for (int j = i + 1; j < arr.length; j++) {
                if (arr[i] + arr[j] == target) {
                    return new int[] { arr[i], arr[j] };
                }
            }
        }
        return new int[] {};
    }
    //Time complexity: O(n)
    public static int[] targetTwoNumberSumOn(int[] arr, int target) {
        /*Assignment:Code goes here to come up with an optimized version Time complexity: O(n)*/
        return new int[] {};
    }
    // Time complexity: O(n*logn)
    public static int[] targetTwoNumberSumnlogn(int[] arr, int target){
        /*Assignment:Code goes here to come up with an optimized version Time complexity: O(n*logn)*/
        return new int[] {};
    }
    public static void main(String[] args) throws Exception {
        int arr[] = {3,5,-4,8,11,1,-1,6};
        int target=13;
        int op1[]=targetTwoNumberSumOn2(arr,target);
        System.out.println(Arrays.toString(op1));
        /*int op2[]=targetTwoNumberSumOn(arr,target);
        System.out.println(Arrays.toString(op2));
        int op3[]=targetTwoNumberSumnlogn(arr,target);
        System.out.println(Arrays.toString(op3));*/
    }

}