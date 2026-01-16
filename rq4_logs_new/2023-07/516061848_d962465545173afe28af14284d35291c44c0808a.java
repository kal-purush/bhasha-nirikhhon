package Easy.LongestCommonPrefix;

import java.util.Arrays;

public class Solution {

    public String longestCommonPrefix(String[] strs) {


        int size = strs.length;


        if (size == 0) return "";

        if (size == 1) return strs[0];

        Arrays.sort(strs);

        int min = Math.min(strs[0].length(), strs[strs.length - 1].length());

        int i = 0;

        while (i<min && strs[0].charAt(i)== strs[size-1].charAt(i))

            i++;

            String result = strs[0].substring(0, i);

        return result;
    }

}