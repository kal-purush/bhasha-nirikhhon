package com.example.demo;

import java.io.*;
import java.util.*;

public class AISecurityAnalyzer {
    public static List<String[]> parseLogFile(String filePath) throws IOException {
        List<String[]> logs = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String line;

        while ((line = reader.readLine()) != null) {
            logs.add(line.split(" "));  // Adjust this for actual log format
        }
        reader.close();
        return logs;
    }
}