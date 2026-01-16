package com.github.lucaseo90.ps.day01;

public class StrStr {
    public int strStr(String haystack, String needle) {
        if (haystack.length() < needle.length()) {
            return -1;
        }

        int index = -1;
        for (int i = 0; i < haystack.length(); i++) {
            if (haystack.charAt(i) == needle.charAt(0) && i + needle.length() <= haystack.length()) {
                index = i;
                for (int j = 0; j < needle.length(); j++) {
                    if (haystack.charAt(i + j) != needle.charAt(j)) {
                        index = -1;
                        break;
                    }
                }
                if (index != -1) {
                    break;
                }
            }
        }
        return index;
    }
}