package com.keepthinker.example.general;

import java.util.*;

public class InterviewCodeBackup {

    /**
     * 代码中的类名、方法名、参数名已经指定，请勿修改，直接返回方法规定的值即可
     *
     *
     * @param words string字符串一维数组
     * @return string字符串
     */
    public String longestWord (String[] words) {
        int maxLength = 0;
        Map<Integer, List<String>> lengthWordsMap = new HashMap<>();
        for (int i = 0; i < words.length; i++) {
            String tempWord = words[i];
            if (tempWord != null && tempWord.length()  >= maxLength) {
                maxLength = words[i].length();
                List<String> list = lengthWordsMap.get(maxLength);
                if (list == null) {
                    list = new ArrayList<>();
                    lengthWordsMap.put(maxLength, list);
                } else {
                    list.add(tempWord);
                }
                list.add(tempWord);
            }
        }

        List<String> maxWords = lengthWordsMap.get(maxLength);
        if (maxWords == null) {
            return null;
        }
        Collections.sort(maxWords);
        return maxWords.get(0);


    }
}