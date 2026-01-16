package com.github.lucaseo90.algo.days21.day14;

public class LongestPalindromicSubstring {
    public String longestPalindrome(String s) {
        if (s.length() <= 1) {
            return s;
        }

        String longestPalindrome = String.valueOf(s.charAt(0));
        for (int i = 1; i < s.length(); i++) {
            // odd case
            longestPalindrome = findPalindromicSubstring(s, i - 1, i + 1, longestPalindrome);

            // even case
            longestPalindrome = findPalindromicSubstring(s, i - 1, i, longestPalindrome);

        }
        return longestPalindrome;
    }

    private String findPalindromicSubstring(String s, int toLeft, int toRight, String longestPalindrome) {
        while (toLeft >= 0 && toRight < s.length()) {
            if (s.charAt(toLeft) == s.charAt(toRight)) {
                if (longestPalindrome.length() < (toRight - toLeft) + 1) {
                    longestPalindrome = s.substring(toLeft, toRight + 1);
                }
                toLeft--;
                toRight++;
            } else {
                break;
            }
        }
        return longestPalindrome;
    }
}