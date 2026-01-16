package com.openpromt.coffeee.swf2023.openpromtserver.util.jakard;

import java.util.ArrayList;
import java.util.List;

public class Jakard {

    public static int inspect(String newPrompt, String existPrompt){
        int constant = 65536;
        List<String> set1 = new ArrayList<>();
        List<String> set2 = new ArrayList<>();
        List<String> intersection = new ArrayList<>();
        List<String> union = new ArrayList<>();

        newPrompt = newPrompt.trim().toLowerCase();
        existPrompt = existPrompt.trim().toLowerCase();

        for(int i=0; i<newPrompt.length()-1; i++){
            char c1 = newPrompt.charAt(i);
            char c2 = newPrompt.charAt(i + 1);
            if(Character.isLetter(c1) && Character.isLetter(c2))
                set1.add("" + c1 + c2);
        }

        for(int i=0; i<existPrompt.length()-1; i++){
            char c1 = existPrompt.charAt(i);
            char c2 = existPrompt.charAt(i + 1);
            if(Character.isLetter(c1) && Character.isLetter(c2))
                set2.add("" + c1 + c2);
        }

        for(String s : set1){
            if(set2.contains(s)){
                intersection.add(s);
                set2.remove(s);
            }
        }

        union.addAll(set1);
        union.addAll(set2);

        for(String s : intersection)
            union.remove(s);

        return intersection.isEmpty() && union.isEmpty() ? constant : (int)Math.floor((double)(intersection.size() * constant) / union.size());
    }
}