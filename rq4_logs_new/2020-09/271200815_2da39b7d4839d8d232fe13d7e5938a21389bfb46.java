package com.practice.hackerRank;

import java.util.Scanner;

public class EnclosingSubstring {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int test = sc.nextInt();
        while (test > 0){
            String A = sc.next();
            String B = sc.next();
            int ans = enclosingSubstring(A, B);
            System.out.println(ans);
            test--;
        }
    }

    private static int enclosingSubstring(String a, String b) {
        int[] countA = new int[26];
        int[] countB = new int[26];
        for (int i = 0; i < a.length(); i++)
            countA[a.charAt(i) - 'a']++;
        int i = 0;
        int j = 0;
        int ans = Integer.MAX_VALUE;
        while (i < b.length() && j < b.length()){
            while (!valid(countA, countB) && j < b.length()){
                countB[b.charAt(j) - 'a']++;
                j++;
            }
            if (valid(countA, countB))
                ans = Math.min(ans, j-i+1);
            while (valid(countA, countB) && i < b.length()){
                countB[b.charAt(i) - 'a']--;
                i++;
                ans = Math.min(ans, j - i+1);
            }
        }
        if (ans == Integer.MAX_VALUE)
            return -1;
        return ans;
    }

    private static boolean valid(int[] countA, int[] countB) {
        for (int i = 0; i < 26; i++){
            if (countA[i] > 0 && countA[i] > countB[i])
                return false;
        }
        return true;
    }
}