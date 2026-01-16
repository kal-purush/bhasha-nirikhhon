package com.github.lucaseo90.algo.day04.twopointer;

import java.util.ArrayList;
import java.util.List;

public class No0557 {
    public String reverseWords(String s) {
        String[] split = s.split(" ");

        List<String> stringList = new ArrayList<>();
        for (int i = 0; i < split.length; i++) {
            char[] subString = split[i].toCharArray();

            int right = 0;
            int left = subString.length - 1;
            while (right < left) {
                swap(subString, right, left);
                right += 1;
                left -= 1;
            }
            stringList.add(new String(subString));
        }

        return String.join(" ", stringList);
    }

    private void swap(char[] arr, int left, int right) {
        char temp = arr[left];
        arr[left] = arr[right];
        arr[right] = temp;
    }
}