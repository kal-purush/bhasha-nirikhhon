package com.practice.hackerRank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;

public class ManachersAlgo {
    public static void main(String[] args)throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int test = Integer.parseInt(br.readLine());
        while (test > 0){
            int n = Integer.parseInt(br.readLine());
            String s = br.readLine();
            int ans = longestPallindrome(s);
            System.out.println(ans);
            test--;
        }
    }

    private static int longestPallindrome(String s) {
        StringBuilder sb = new StringBuilder("#");
        for (int i = 0; i < s.length(); i++){
            sb.append(s.charAt(i)).append("#");
        }
        Integer[] p = new Integer[sb.length()];
        int l = 0;
        int r = -1;
        int n = sb.length();
        for (int i = 0; i < n; i++){
            int k;
            if (i > r)
                k = 0;
            else {
                int j = l + r - i;
                k = Math.min(p[j], r - i);
            }
            while (i-k >= 0 && i+k < n && sb.charAt(i-k) == sb.charAt(i+k))
                k++;
                --k;
            p[i] = k;
            if (i + k > r){
                l = i - k;
                r = i + k;
            }
        }
        return Collections.max(Arrays.asList(p));
    }
}