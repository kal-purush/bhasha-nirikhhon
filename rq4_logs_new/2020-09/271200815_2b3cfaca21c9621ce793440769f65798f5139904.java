package com.practice.hackerRank;

import java.util.Scanner;

public class LongestPallindromicSubstring {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int test = sc.nextInt();
        while (test > 0) {
            int n = sc.nextInt();
            String s = sc.next();
            int ans = longestPallindromicString(s);
            System.out.println(ans);
            test--;
        }
    }

    private static int longestPallindromicString(String s) {
        if (s.length() == 1)
            return 1;
        int oddSubstring = checkOdd(s);
        int evenSubstring = checkEven(s);
        return Math.max(oddSubstring, evenSubstring);
    }

    private static int checkEven(String s) {
        int ans = Integer.MIN_VALUE;
        int i = 0;
        while (i < s.length()-1){
            int p1 = i;
            int p2 = i+1;
            while (p1 >= 0 && p2 < s.length() && s.charAt(p1) == s.charAt(p2)){
                p1--;
                p2++;
            }
           ans = Math.max(ans, p2-p1-1);
            i++;
        }
        return ans;
    }
    private static int checkOdd(String s) {
        int ans = Integer.MIN_VALUE;
        int i = 1;
        while (i < s.length()-1){
            int p1 = i-1;
            int p2 = i+1;
            while (p1 >= 0 && p2 < s.length() && s.charAt(p1) == s.charAt(p2)){
                p1--;
                p2++;
            }
            ans = Math.max(ans, p2-p1-1);
            i++;
        }
        return ans;
    }
}