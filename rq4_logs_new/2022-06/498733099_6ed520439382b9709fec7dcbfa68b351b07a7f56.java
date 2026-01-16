import java.util.*;
// Time Complexity : O(n * k) where n is the no of strs and k is length of the longest string
// Space Complexity : O(n * k)
// Did this code successfully run on Leetcode : Yes
// Any problem you faced while coding this : No

public class Problem1 {
    class Solution {

        // In this approach, I am first finding the count of each letter in a string and then create a unique representation string from it, which represents a group of Anagrams
        public List<List<String>> groupAnagrams(String[] strs) {
            Map<String, List<String>> groups = new HashMap<>();
            int[] letters = new int[26];

            for (String s: strs) {
                char[] charArr = s.toCharArray();

                // the commented code below give (n* (k *log k) time complexity
                //Arrays.sort(charArr);

                // if (!groups.containsKey(String.valueOf(charArr))) {
                //     groups.put(String.valueOf(charArr), new ArrayList<String>());
                // }

                // groups.get(String.valueOf(charArr)).add(s);


                // resetting the count for new string
                Arrays.fill(letters, 0);
                for (char c : charArr) {
                    letters[c - 'a']++;
                }

                String rep = "";

                // creating a unique representation of group of anagrams
                for (int i : letters) {
                    rep += "#" + i;
                }

                if (!groups.containsKey(rep)) {
                    groups.put(rep, new ArrayList<String>());
                }

                groups.get(rep).add(s);
            }

            return new ArrayList(groups.values());
        }
    }
}