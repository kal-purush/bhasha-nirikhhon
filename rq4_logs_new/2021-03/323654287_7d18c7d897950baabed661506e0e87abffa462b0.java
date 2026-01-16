package challenge.y2021_week11;

// https://leetcode.com/problems/short-encoding-of-words/
// Short Encoding of Words

import java.util.*;

public class Q06 {
    public int minimumLengthEncoding(String[] words) {
        Arrays.sort(words, (o1, o2) -> -Integer.compare(o1.length(), o2.length()));

        StringBuilder sb = new StringBuilder();
        for (String word : words) {
            String s = word + "#";
            if (!sb.toString().contains(s)) {
                sb.append(s);
            }
        }

        return sb.length();
    }
}