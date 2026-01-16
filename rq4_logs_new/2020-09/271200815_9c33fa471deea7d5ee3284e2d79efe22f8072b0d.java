package com.practice.interviewbit;

import java.util.Scanner;

public class CompareVersionNumbers {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String a = sc.next();
        String b = sc.next();
        int ans = compareVersion(a, b);
        System.out.println(ans);
    }

    private static int compareVersion(String a, String b) {
       String[] arr = a.split("\\.");
       String[] brr = b.split("\\.");
       int compare = validate(arr, brr);
       return compare;
    }

    private static int validate(String[] arr, String[] brr) {
        if (arr[0].equals(""))
            arr[0] = "0";
        if (brr[0].equals(""))
            brr[0] = "0";
        int i = 0;
        int j = 0;
        while (i < arr.length && j < brr.length){
            int k = Integer.parseInt(arr[i]);
            int l = Integer.parseInt(brr[j]);
            if (k < l)
                return -1;
            else if (k > l)
                return 1;
            i++;
            j++;
        }
        if (i == arr.length && j == brr.length)
            return 0;
        return -1;
    }
}