package com.practice.interviewbit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;

public class LongestCommonPrefix {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String[] arr = br.readLine().split(" ");
        ArrayList<String> list = new ArrayList<>();
        Collections.addAll(list, arr);
        String ans = longestPrefix(list);
        System.out.println(ans);
    }

    private static String longestPrefix(ArrayList<String> list) {
        if (list.size() == 1)
            return list.get(0);
        StringBuilder prefix = new StringBuilder("");
        String s = list.get(0);
        String s1 = list.get(1);
        int p1 = 0;
        int p2 = 0;
        while (p1 < s.length() && p2 < s1.length()){
            if (s.charAt(p1) == s1.charAt(p2)) {
                prefix.append(s.charAt(p1));
                p1++;
                p2++;
            }
            else
                break;
        }
        list.remove(0);
        list.remove(0);
        for (String str : list){
            p1 = 0;
            p2 = 0;
            while (p1 < prefix.length() && p2 < str.length()){
                if (prefix.charAt(p1) == str.charAt(p2)){
                    p1++;
                    p2++;
                }
                else
                    break;
            }
            prefix.replace(p1, prefix.length(), "");
        }
        return prefix.toString();
    }
}