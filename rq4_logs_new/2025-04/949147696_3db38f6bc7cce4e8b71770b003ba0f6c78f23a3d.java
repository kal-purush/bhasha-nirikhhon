package com.ArrayProgram;

public class FirstMaximum {
    public static void main(String[] args) {
        int[] arr = {1, 4, 2, 5, 790, 3, 0, 80};
        System.out.println(maximum(arr));
        System.out.println(secondMax(arr));

    }

    public static int maximum(int[] arr) {
        int n = arr.length;
        int max = Integer.MIN_VALUE;
        for (int i = 0; i < n; i++) {
            if (arr[i] > max) {
                max = arr[i];
            }
        }
        return max;

    }
    public static int secondMax(int[] arr){
        int n = arr.length;
        int max = Integer.MIN_VALUE;
        for (int i = 0; i < n; i++) {
            if (arr[i] > max) {
                max = arr[i];
            }
        }
        System.out.println(max);
        int second = Integer.MIN_VALUE;
        for(int i =0; i<n; i++){
            if (arr[i] >second & arr[i]<max){
                second = arr[i];
            }
        }
        return second;
    }
}