package com.practice.interviewbit;

import java.util.Scanner;

public class ExcelColumnNumber {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        String s = sc.next();
        int ans = findcolumnNumber(s);
        System.out.println(ans);
    }

    private static int findcolumnNumber(String s) {
        int ans = 0;
        for (int i = 0; i < s.length(); i++){
            ans += s.charAt(i)-'A' + 1;
            ans *= 26;
        }
        return ans/26;
    }
}