package algorithms.bst;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * Given an input string of 0 or more digits '2' - '9', list out all the possible
 * strings from a typical phonepad digit-to-letter conversion (below), one per line
 *    2: ABC
 *    3: DEF
 *    4: GHI
 *    5: JKL
 *    6: MNO
 *    7: PQRS
 *    8: TUV
 *    9: WXYZ
 *
 */
public class TelephoneKeypad {

    private static HashMap<Character, List<Character>> keypadMap = generateKeypadMap();
    private static int counter = 0;

    public static void main(String[] args) {
        generateAllPossibleStrings("22777886644666"); // bruno
//        generateAllPossibleStrings("278646"); // input do kova
//        generateAllPossibleStrings("222"); // ab
    }

    private static void generateAllPossibleStrings(String s) {
        generateAllPossibleStrings(s, 0, new StringBuilder());
        System.out.println("total: " + counter);
    }

    private static void generateAllPossibleStrings(String s, int start, StringBuilder sb) {
        if (isSolution(s, start)) {
            System.out.println(sb.toString());
            counter++;
        } else {
            // generate all possible leafs
            char number = s.charAt(start);
            List<Character> possibleLetters = keypadMap.get(number);
            for (int i = 0; i < possibleLetters.size(); i++) {
                sb.append(possibleLetters.get(i));
                generateAllPossibleStrings(s, start+i+1, sb);

                // backtrack
                sb.deleteCharAt(sb.length() - 1);

                if (start + i + 1 >= s.length() || s.charAt(start + i + 1) != number) break;
            }
        }
    }

    private static boolean isSolution(String s, int start) {
        return start >= s.length();
    }

    private static HashMap<Character, List<Character>> generateKeypadMap() {
        HashMap<Character, List<Character>> map = new HashMap<>(8);
        ArrayList<Character> list = new ArrayList<>();
        list.add('a');
        list.add('b');
        list.add('c');
        map.put('2', list);

        list = new ArrayList<>();
        list.add('d');
        list.add('e');
        list.add('f');
        map.put('3', list);

        list = new ArrayList<>();
        list.add('g');
        list.add('h');
        list.add('i');
        map.put('4', list);

        list = new ArrayList<>();
        list.add('j');
        list.add('k');
        list.add('l');
        map.put('5', list);

        list = new ArrayList<>();
        list.add('m');
        list.add('n');
        list.add('o');
        map.put('6', list);

        list = new ArrayList<>();
        list.add('p');
        list.add('q');
        list.add('r');
        list.add('s');
        map.put('7', list);

        list = new ArrayList<>();
        list.add('t');
        list.add('u');
        list.add('v');
        map.put('8', list);

        list = new ArrayList<>();
        list.add('w');
        list.add('x');
        list.add('y');
        list.add('z');
        map.put('9', list);

        return map;
    }
}