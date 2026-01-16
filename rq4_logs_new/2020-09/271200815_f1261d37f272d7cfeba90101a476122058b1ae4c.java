package com.practice.hackerRank.stack;

import java.io.*;
import java.util.Arrays;
import java.util.Stack;

public class RectangularAreaUnderHistogram {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        int test = Integer.parseInt(br.readLine());
        while (test > 0){
            int n = Integer.parseInt(br.readLine());
            int[] arr = new int[n];
            String[] sarr = br.readLine().split(" ");
            for (int i = 0; i < n; i++)
                arr[i] = Integer.parseInt(sarr[i]);
            int ans = maximumArea(arr, n);
            bw.write(ans + "");
            bw.newLine();
            test--;
        }
        bw.flush();
    }

    private static int maximumArea(int[] arr, int n) {
        int[] p1 = firstSmallestOnRight(arr, n);
        int[] p2 = firstSmallestOnLeft(arr, n);
        int ans = 0;
        for (int i = 0; i < n; i++){
           int range = Math.abs(p2[i] - p1[i]) - 1;
           ans = Math.max(ans, range*arr[i]);
        }
        return ans;
    }

    private static int[] firstSmallestOnLeft(int[] arr, int n) {
        int[] p2 = new int[n];
        Arrays.fill(p2, -1);
        Stack<Integer> stack = new Stack<>();
        for (int i = n-1; i >= 0; i--){
            while (!stack.isEmpty() && arr[stack.peek()] > arr[i]){
                p2[stack.peek()] = i;
                stack.pop();
            }
            stack.push(i);
        }
        return p2;
    }

    private static int[] firstSmallestOnRight(int[] arr, int n) {
        int[] p1 = new int[n];
        Arrays.fill(p1, n);
        Stack<Integer> stack = new Stack<>();
        for (int i = 0; i < n; i++){
            while (!stack.isEmpty() && arr[stack.peek()] > arr[i]){
                p1[stack.peek()] = i;
                stack.pop();
            }
            stack.push(i);
        }
        return p1;
    }
}