package com.hackerrank.Java.DataStructures.Java1DArrayPart2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class Solution {

    public static void main(String[] args) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));

        int q = Integer.parseInt(bufferedReader.readLine().trim());
        Map<String[], String[]> allGames = new HashMap<>();

        for (int i = 0; i < q; i++) {
            String[] n_leap = bufferedReader.readLine().replaceAll("\\s+$", "").split(" ");
            String[] game = bufferedReader.readLine().replaceAll("\\s+$", "").split(" ");
            allGames.put(n_leap, game);
        }
    }
}