package Easy.Find_the_First_Occurance_in_The_String;

import java.util.concurrent.LinkedBlockingDeque;

public class Solution_2 {
    public static int strStr(String haystack, String needle) {

        if (haystack.length() < needle.length())
            return -1;

        if (haystack.length() == needle.length())
            if (!haystack.equals(needle))
                return -1;
            else return 0;


        return haystack.indexOf(needle);


    }


    public static void main(String[] args) {

        System.out.println(strStr("sdfsdfsadbutsad", "sdaed"));

    }


}