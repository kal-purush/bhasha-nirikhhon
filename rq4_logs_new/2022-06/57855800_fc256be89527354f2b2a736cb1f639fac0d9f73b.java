package com.github.lucaseo90.medium;

import java.util.HashSet;

public class No0003 {
    public int lengthOfLongestSubstring(String s) {
        HashSet<Character> set = new HashSet<>();

        int maxLength = 0;
        for (int i = 0; i < s.length(); i++) {
            for (int j = i; j < s.length(); j++) {
                char ch = s.charAt(j);
                if (set.contains(ch)) {
                    break;
                } else {
                    set.add(ch);
                }
            }
            maxLength = Math.max(maxLength, set.size());
            set.clear();
        }

        return maxLength;
    }
}