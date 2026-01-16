package com.leetcode.cn.solution1_50;

public class Solution58 {

    public int lengthOfLastWord(String s) {
        // 是否遇到英文字母
        boolean flag = false;
        int wordLength = 0;
        for (int i = s.length() - 1; i >= 0; i--) {
            if (!flag && s.charAt(i) != ' ') {
                flag = true;
            }
            if (flag) {
                if (s.charAt(i) != ' ') {
                    wordLength++;
                } else {
                    break;
                }
            }
        }
        return wordLength;
    }
}