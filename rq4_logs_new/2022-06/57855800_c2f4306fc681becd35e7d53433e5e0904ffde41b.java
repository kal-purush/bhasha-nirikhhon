package com.github.lucaseo90.easy;

import java.util.HashMap;
import java.util.Map;

public class No0819 {
    public String mostCommonWord(String paragraph, String[] banned) {
        paragraph = paragraph.toLowerCase();
        paragraph = paragraph.replaceAll("[^a-zA-Z0-9]", " ");

        String[] split = paragraph.split(" ");
        HashMap<String, Integer> map = new HashMap<>();
        for (String word : split) {
            word = word.trim();

            if (!map.containsKey(word)) {
                map.put(word, 1);
            } else {
                map.put(word, map.get(word) + 1);
            }
        }

        map.remove("");
        for (String ban : banned) {
            map.remove(ban.toLowerCase());
        }

        String maxString = "";
        int maxCount = 0;
        for (Map.Entry<String, Integer> entry: map.entrySet()) {
            if(entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                maxString = entry.getKey();
            }
        }

        return maxString;
    }
}