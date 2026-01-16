package com.practice.interviewbit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class CountingSubarrays {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String[] arr = br.readLine().split(" ");
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < arr.length; i++)
            list.add(i, Integer.parseInt(arr[i]));
        int b = Integer.parseInt(br.readLine());
        int ans = countSubarray(list, b);
        System.out.println(ans);
    }

    private static int countSubarray(ArrayList<Integer> list, int b) {
       int sum = list.get(0);
       int count = 0;
       int p1 = 0;
       int p2 = 1;
       while (true){
           while (sum < b && p2 < list.size()){
               sum += list.get(p2);
               p2++;
           }
           int n = p2-p1-1;

       }
    }
}