package com.practice.hackerRank;

import java.util.Scanner;

public class AnagramsEasy {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        int test = sc.nextInt();
        while (test > 0) {
            String s = sc.next();
            String p = sc.next();
            boolean check = isAnagram(s, p);
            if (check)
                System.out.println("True");
            else
                System.out.println("False");
            test--;
        }
    }

    private static boolean isAnagram(String s, String p) {
        int[] countS = new int[26];
        int[] countP = new int[26];
        for (int i = 0; i < s.length(); i++)
            countS[s.charAt(i)-'a']++;
        for (int i = 0; i < p.length(); i++)
            countP[p.charAt(i)-'a']++;
        for (int i = 0; i < 26; i++)
            if (countS[i] != countP[i])
                return false;
        return true;
    }
}