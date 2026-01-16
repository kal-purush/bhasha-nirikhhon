package rish.leets.contests.weekly;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/*
 *            Weekly Contest 354
 * 
 * Problem #: 2781
 * Problem link:https://leetcode.com/problems/length-of-the-longest-valid-substring/
 * Contest link: https://leetcode.com/contest/weekly-contest-354/
 * 
 * Date Attempted: 20/07/2023
 * 
 * @author Rishabh Soni
 * 
 */
public class LongestNonForbiddenSubstring {

    public static int longestValidSubstring(String word, List<String> forbidden) {

        Set<String> forbiddenSet = new HashSet<>(forbidden);
        int forbMax = forbiddenSet.stream().max(Comparator.comparingInt(String::length)).get().length();

        int max = 0;
        int len = word.length();

        // Points to index where forbidden word starts
        int right = len;

        for (int i = len - 1; i >= 0; i--) {

            StringBuilder sb = new StringBuilder();
            for (int j = i; j < i + forbMax & j < right; j++) {
                sb.append(word.charAt(j));
                if (forbiddenSet.contains(sb.toString())) {
                    right = j;
                    break;
                }
                System.out.println(sb.toString());
            }

            System.out.println("Right : " + right);
            max = Math.max(max, right - i);
        }

        return max;
    }

    public static void main(String[] args) {

        String word = "cbaaaabc";
        List<String> forbidden = Arrays.asList("aaa", "cb");

        System.out.println(longestValidSubstring(word, forbidden));
    }

}