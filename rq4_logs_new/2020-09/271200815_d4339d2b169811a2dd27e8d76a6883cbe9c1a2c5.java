package com.practice.hackerRank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;

public class ArrayManipulation {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String[] arr = br.readLine().split(" ");
        int n = Integer.parseInt(arr[0]);
        int m = Integer.parseInt(arr[1]);
        int[][] queries = new int[m][3];
        for (int i = 0; i < m; i++) {
            String[] brr = br.readLine().split(" ");
            for (int j = 0; j < 3; j++) {
                queries[i][j] = Integer.parseInt(brr[j]);
            }
        }
        long ans = arrayManipulation(n, queries);
        System.out.println(ans);
    }

    private static long arrayManipulation(int n, int[][] queries) {
        long[] arr = new long[n+1];
        for (int[] query : queries){
            int startIndex = query[0];
            int lastIndex = query[1];
            int value = query[2];
            arr[startIndex] += value;
            if (lastIndex + 1 <= n)
                arr[lastIndex+1] += -value;
        }
        for (int i = 1; i <= n; i++)
            arr[i] += arr[i-1];
        return Arrays.stream(arr).max().getAsLong();
    }
}