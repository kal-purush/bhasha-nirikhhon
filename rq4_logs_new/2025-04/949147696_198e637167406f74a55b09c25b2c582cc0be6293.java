package com.ArrayProgram;

import java.util.Arrays;
import java.util.HashSet;

public class Duplicate {
    public static void main(String[] args) {
        int[] arr = {1,5,2,4,1,5};
//                   1,1,2,4,5,5
        Arrays.sort(arr);
        duplicate(arr);


    }
    public static void duplicate(int[] arr){
        int n = arr.length;
        HashSet<Integer> set = new HashSet();
        for(int i = 0; i<n-1; i++){
            if (arr[i] == arr[i+1]){
                set.add(i);
            }
        }
        System.out.println(set);
        for(Integer i:set){
            System.out.println(i);
        }


    }
}