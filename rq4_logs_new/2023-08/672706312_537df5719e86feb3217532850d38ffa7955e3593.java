package com.openpromt.coffeee.swf2023.openpromtserver.util.jaccard;

import java.util.HashSet;
import java.util.Set;

public class Jaccard {

/*    public static int inspect(String newPrompt, String existPrompt){
        int constant = 65536;
        List<String> set1 = new ArrayList<>();
        List<String> set2 = new ArrayList<>();
        List<String> intersection = new ArrayList<>();
        Set<String> union = new HashSet<>();

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

        return (intersection.isEmpty() && union.isEmpty()) ? constant : (int)Math.floor((double)(intersection.size() * constant) / union.size());
    }*/

    public static double jaccardSimilarity(String newPrompt, String existPrompt){
        Set<String> set1 = tokenizeString(newPrompt);
        Set<String> set2 = tokenizeString(existPrompt);

        Set<String> union = new HashSet<>(set1);
        union.addAll(set2);

        Set<String> intersection = new HashSet<>(set1);
        intersection.retainAll(set2);

        int unionSize = union.size();
        int intersectionSize = intersection.size();

        return (double) intersectionSize / unionSize * 100;
    }

    private static Set<String> tokenizeString(String str){
        Set<String> tokens = new HashSet<>();
        String[] words = str.toLowerCase().split("\\s+");
        for(String word : words)
            tokens.add(word);
        return tokens;
    }
}