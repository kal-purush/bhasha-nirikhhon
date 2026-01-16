package com.github.lucaseo90.ps.day12;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class No0438 {
    public List<Integer> findAnagrams(String s, String p) {
        if (p.length() > s.length()) {
            return Collections.emptyList();
        }

        HashMap<Character, Integer> pMap = new HashMap<>();
        for (int i = 0; i < p.length(); i++) {
            char ch = p.charAt(i);
            if (pMap.containsKey(ch)) {
                pMap.put(ch, pMap.get(ch) + 1);
            } else {
                pMap.put(ch, 1);
            }
        }

        List<Integer> answer = new ArrayList<>();
        for (int i = 0; i < s.length(); i++) {
            if (pMap.containsKey(s.charAt(i)) && s.length() - i >= p.length()) {
                HashMap<Character, Integer> sMap = new HashMap<>();
                for(int j = i; j < i + p.length(); j++){
                    char ch = s.charAt(j);
                    if (sMap.containsKey(ch)) {
                        sMap.put(ch, sMap.get(ch) + 1);
                    } else {
                        sMap.put(ch, 1);
                    }
                }
                if(pMap.equals(sMap)) {
                    answer.add(i);
                    i += p.length();
                }
            }
        }
        return answer;
    }
}