package com.mmdj.textanalyzer.analisis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class WordsCountAndSort {
    /**
     * counting an amount of words and sorting
     * @param strArray words in array
     * @return List of sorted and counted words
     */
    public static List<Map.Entry<String, Integer>> countAndSort(String[] strArray) {
        HashMap<String, Integer> map = new HashMap<>();
        fillMapAndCounting(map, strArray);
        return sortingList(map);
    }

    /**
     * Filling list from map and sorting
     *
     * @param map with counted words
     * @return List for sorting
     */
    private static List<Map.Entry<String, Integer>> sortingList(HashMap<String, Integer> map) {
        List<Map.Entry<String, Integer>> res = new ArrayList<>();

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            res.add(entry);
        }
        Collections.sort(res, new EntriesComparator());
        return res;
    }

    /**
     * counting the amount of words and filling the hashMap
     *
     * @param map      empty
     * @param strArray uncounted words
     */
    private static void fillMapAndCounting(HashMap<String, Integer> map, String[] strArray) {
        for (String string : strArray) {
            int value = 1;
            if (map.containsKey(string)) {
                value = map.get(string);
                value++;
            }
            map.put(string, value);//word,count
        }
    }

}