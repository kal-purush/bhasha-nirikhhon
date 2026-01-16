package com.practice.hackerRank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;

public class ReverseTheSentence {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int test = Integer.parseInt(br.readLine());
        while (test > 0){
            String s = br.readLine();
            String ans = reverseTheSentence(s);
            System.out.println(ans);
            test--;
        }
    }

    private static String reverseTheSentence(String s) {
        String[] arr = s.split(" ");
        Collections.reverse(Arrays.asList(arr));
        StringBuilder sb = new StringBuilder();
        for (String str : arr){
            sb.append(str);
            sb.append(" ");
        }
        return sb.toString();
    }
}