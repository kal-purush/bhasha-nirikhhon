package com.practice.hackerearth;

import java.util.Scanner;

public class Problem3 {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int test = sc.nextInt();
        while (test > 0){
            int k = sc.nextInt();
            int n = sc.nextInt();
            long ans = ktree(k, n);
            System.out.println(ans);
            test--;
        }
    }

    private static long ktree(int k, int n) {
        long nodes = ((long)Math.pow(k, n+1) - 1)/(k-1);
        long ans = 0;
        while (nodes != 0){
            ans += nodes%10;
            nodes /= 10;
        }
        return ans;
    }
}