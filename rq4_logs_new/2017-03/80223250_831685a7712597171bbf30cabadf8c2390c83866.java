package arrays.practice;

import java.util.HashMap;
import java.util.Map;

/**
 * Given a string, write a function to check if it is a permutation of a palindrome. A palindrome is a word or phrase
 * that is the same forwarsd and backwards. A permutation is a rearrangement of letters.
 */
public class PalindromePerumutation {
    public static void main(String[] args) {
    }
    public static boolean palindromePermutation(final String input) {
        final Map<Character, Integer> map = new HashMap<>();
        for(char c : input.toCharArray()) {
            if(c == ' ') {
                continue;
            }
            insertOrUpdateMap(map , c);
        }

        boolean flag = true;
        for(Map.Entry entry: map.entrySet()) {
            if((int) entry.getValue() %2 == 1) {
                if(!flag) {
                    return false;
                } else {
                    flag = false;
                }
            }
        }

        return true;
    }

    private static void insertOrUpdateMap(final Map<Character, Integer> map, char c) {
        if(!map.containsKey(c)) {
            map.put(c,1);
        } else {
            map.put(c, map.get(c) + 1);
        }
    }
}