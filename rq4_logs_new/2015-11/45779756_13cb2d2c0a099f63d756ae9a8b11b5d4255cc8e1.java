package codingtests.romantointeger;

import java.util.TreeMap;

public class RomanToInteger {

    public int convertRomanToInteger(String romanNr) {
        TreeMap<Character, Integer> converterMap = new TreeMap<Character, Integer>() {{
            put('M', 1000);
            put('D', 500);
            put('C', 100);
            put('L', 50);
            put('X', 10);
            put('V', 5);
            put('I', 1);
        }};

        Integer value = 0;

        char[] chars = romanNr.toCharArray();

        for (int i = 0; i < chars.length; i++) {
            int current = converterMap.get(chars[i]);

            if (i < chars.length-1 && current < converterMap.get(chars[i+1])){
                value -= current;
            } else {
                value += current;
            }
        }
        return value;
    }
}