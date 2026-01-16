package com.kuldeep.algorithims;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.groupingBy;

/**
 * @author <a href="kuldeepyadav7291@gmail.com">Kuldeep</a>
 */
public class GroupAnagramsApp {
    public static void main(String[] args) {
        String[] strs = {"yo", "act", "flop", "tac", "foo", "cat", "oy", "olfp"};
        List<List<String>> groups = groupAnagrams(Arrays.asList(strs));

        System.out.println(groups);
    }

    private static List<List<String>> groupAnagrams(List<String> list) {
        Collection<List<String>> groups = list.stream()
                                              .collect(groupingBy(GroupAnagramsApp::sort))
                                              .values();
        return new ArrayList<>(groups);
    }

    private static String sort(String string) {
        char[] chrs = string.toCharArray();
        Arrays.sort(chrs);
        return new String(chrs);
    }
}