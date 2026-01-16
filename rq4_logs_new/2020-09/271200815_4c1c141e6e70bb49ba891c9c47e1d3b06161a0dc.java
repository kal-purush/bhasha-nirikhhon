package com.practice.contest;

import java.io.*;
import java.util.HashMap;
import java.util.Set;

public class DeadMensChest {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out));
        int test = Integer.parseInt(br.readLine());
        while (test > 0){
            HashMap<Character, Integer> map = new HashMap<>();
            String s = br.readLine();
            String winner = findWinner(s, map);
            System.out.println(winner);
            test--;
        }
    }

    private static String findWinner(String s, HashMap<Character, Integer> map) {
        if (s.length() == 1)
            return "JACK";
        for (int i = 0; i < s.length(); i++){
            if (map.containsKey(s.charAt(i)))
                map.put(s.charAt(i), map.get(s.charAt(i)) + 1);
            else
                map.put(s.charAt(i), 1);
        }
        if (valid(map))
            return "JACK";
        boolean flag = true;
        for (int i = 0; i < s.length(); i++){
            map.put(s.charAt(i), map.get(s.charAt(i)) - 1);
            if (map.get(s.charAt(i)) == 0)
                map.remove(s.charAt(i));
            flag = !flag;
            if (valid(map)){
                if (flag)
                    return "JACK";
                else
                    return "DAVY JONES";
            }
        }
        return null;
    }

    private static boolean valid(HashMap<Character, Integer> map) {
        if (map.size() == 1)
            return true;
        int countEven = 0;
        int countOdd = 0;
        Set<Character> set = map.keySet();
        for(char c : set){
            if (map.get(c) %2 == 0)
                countEven++;
            else
                countOdd++;
        }
        if ((countEven > 0 && countOdd == 0) || (countEven > 0 && countOdd == 1))
            return true;
        return false;
    }
}