package org.example;

import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;

import static java.lang.Character.isLetter;

public class Main {
    public static void main(String[] args)
    {
        //checking if argument exists
        if(args[0] == null){
            System.out.println("Please provide a word as an argument");
            return;
        }
        String str = args[0].toLowerCase();
        HashMap <Character, Integer> charMap = new HashMap<>();

        createMap(charMap, str);
        displayMapSorted(charMap);
    }

    //Create a hashmap and filling it with data, accepts two arguments:
    //HashMap <Character, Integer> map - our empty map that was created in main
    //String str - our argument that we passed while executing program
    public static void createMap(HashMap <Character, Integer> map, String str){
        int count;
        for (int i = 0; i < str.length(); i++)
        {
            //checking if char is a letter
            if(isLetter(str.charAt(i))){
                //checking if its first occurrence of a char
                if(map.containsKey(str.charAt(i)))
                {
                    //Getting a count element from a map by using char from string
                    count = map.get(str.charAt(i));
                    //Overwriting existing count value by +1
                    map.put(str.charAt(i), ++count);
                }
                else
                {
                    //Put char as a key and 1 as a count because it's a first occurrence
                    map.put(str.charAt(i),1);
                }
            }
        }
    }

    //Display map content sorted by keys, accepts one argument:
    //HashMap <Character, Integer> map - our map that was filled in createMap function before
    private static void displayMapSorted(HashMap <Character, Integer> map){
        // get keys
        Set<Character> keys = map.keySet();

        // sort keys
        TreeSet<Character> sortedKeys = new TreeSet<>(keys);

        // output loop
        for (Character key : sortedKeys)
        {
            System.out.println( key + " = " + map.get(key));
        }
    }
}