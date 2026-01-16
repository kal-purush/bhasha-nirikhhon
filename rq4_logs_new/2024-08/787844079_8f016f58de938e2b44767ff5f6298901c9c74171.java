//https://leetcode.com/problems/keyboard-row/
package algorithms.easy;

import java.util.ArrayList;
import java.util.List;

public class KeyboardRow {
    public static void main(String[] args) {
        KeyboardRow keyboard = new KeyboardRow();
        System.out.println("Output:\t" + keyboard.findWordsOptimized(new String[]{"Hello", "Alaska", "Dad", "Peace"}));
        System.out.println("Output:\t" + keyboard.findWords(new String[]{"omk"}));
        System.out.println("Output:\t" + keyboard.findWordsOptimized(new String[]{"adsdf", "sfd"}));
    }

    public String[] findWordsOptimized(String[] words) {
        String rowMapping = "12210111011122000010020202";
        List<String> list = new ArrayList<>();

        for (String word : words) {
            String lowerWord = word.toLowerCase();
            char row = rowMapping.charAt(lowerWord.charAt(0) - 'a');
            boolean sameRow = true;

            for (char ch : lowerWord.toCharArray()) {
                if (rowMapping.charAt(ch - 'a') != row) {
                    sameRow = false;
                    break;
                }
            }
            if (sameRow) list.add(word);
        }
        return list.toArray(new String[list.size()]);
    }

    public String[] findWords(String[] words) {
        String row1 = "qwertyuiop";
        String row2 = "asdfghjkl";
        String row3 = "zxcvbnm";
        List<String> list = new ArrayList<>();

        for (String word : words) {
            int rowOne = 0;
            int rowTwo = 0;
            int rowThree = 0;
            for (char ch : word.toLowerCase().toCharArray()) {
                if (row1.contains(ch + "")) rowOne++;
                else if (row2.contains(ch + "")) rowTwo++;
                else if (row3.contains(ch + "")) rowThree++;
            }
            if (rowOne == word.length() || rowTwo == word.length() || rowThree == word.length()) list.add(word);
        }
        return list.toArray(new String[list.size()]);
    }
}