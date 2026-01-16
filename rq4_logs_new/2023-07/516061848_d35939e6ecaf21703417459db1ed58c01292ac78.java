package Easy.Find_the_First_Occurance_in_The_String;

import java.util.Arrays;

public class Solution {

//    Input: haystack = "sadbutsad", needle = "sad"
//    Output: 0
//    Explanation: "sad" occurs at index 0 and 6.
//    The first occurrence is at index 0, so we return 0.


    public static int strStr(String haystack, String needle) {
        int m = haystack.length();
        int n = needle.length();


        for (int i = 0; i < (m - n) + 1; i++) {
            if (haystack.charAt(i) == needle.charAt(0)) {
                if (haystack.substring(i, n + i).equals(needle))
                    return i;
            }
        }
        return -1;
    }


    public static void main(String[] args) {

        strStr("sadbutsad", "sad");


    }


}