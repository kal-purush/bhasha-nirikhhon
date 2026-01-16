package com.company;

import java.util.*;
import java.io.File;
import java.io.FileNotFoundException;

public class CsvReader {
    public static ArrayList<String> read(String path) throws FileNotFoundException {
        ArrayList<String> data = new ArrayList<>();
        Scanner sc = new Scanner(new File(path));

        String[] header = sc.nextLine().split(",");
        while (sc.hasNextLine()) {
            ArrayList<String> row = new ArrayList<>();

            String[] line = sc.nextLine().split(",");
            for (int i = 1; i < line.length; i++) {
                row.add(line[i]);
            }
        }
        return data;
    }
}