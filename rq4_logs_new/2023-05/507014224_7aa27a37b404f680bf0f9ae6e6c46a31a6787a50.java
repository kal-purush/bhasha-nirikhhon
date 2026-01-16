package com.discphy.code.controller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ProgrammerController {
    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String[] input = reader.readLine().trim().split(",");

        System.out.println(solution());
    }

    public static int solution() {
        int answer = 0;

        return answer;
    }
}