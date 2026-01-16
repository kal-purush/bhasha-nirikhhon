package adventofcode.day5;

import adventofcode.common.FileUtil;

import java.util.*;

public class Day5 {
    private static Map<Character, Character> dict;
    static {
        dict = new HashMap<>();
        for (char a = 'a'; a <= 'z'; a++) {
            dict.put(a, Character.toUpperCase(a));
            dict.put(Character.toUpperCase(a), a);
        }
    }

    public Day5() {
        try {
            String input = FileUtil.readInputFile(this.getClass()).get(0);
            System.out.println(solvePart1(input));
            System.out.println(solvePart2(input));
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    private static String collapseAll(String input) {
        int index = 0;
        StringBuilder data = new StringBuilder(input);
        while (true) {
            if (index == data.length()-1) {
                break;
            }

            if (index < 0) {
                index = 0;
            }

            if (charactersMatch(data.charAt(index), data.charAt(index+1))) {
                data.delete(index, index+2);
                index = 0;
            } else {
                index++;
            }
        }

        return data.toString();
    }

    static int solvePart1(String input)  {
        return collapseAll(input).length();
    }

    static int solvePart2(String input) {
        List<String> collapsedStrings = new ArrayList<>();
        for (char a = 'a'; a < 'z'; a++) {
            String regexPattern = "[" + a + Character.toUpperCase(a) + "]";
            String collapsedString = collapseAll(input.replaceAll(regexPattern, ""));
            collapsedStrings.add(collapsedString);
        }

        return Collections.min(collapsedStrings, Comparator.comparing(String::length)).length();
    }

    private static boolean charactersMatch(char left, char right) {
        return dict.containsKey(left) && dict.get(left) == right;
    }

    public static void main(String[] args) {
        new Day5();
    }
}