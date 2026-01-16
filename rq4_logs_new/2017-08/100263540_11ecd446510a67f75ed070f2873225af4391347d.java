package com.tsystems;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class csvReader {
    public static void main(String[] args) {
        String testFile = "/home/matraiv/quiz_game-t-systems-/src/test.csv";
        File fileName = new File(testFile);

        //two dimensional arrayList
        List<List<String>> lines = new ArrayList<>();
        Scanner inputStream;

        try{
            inputStream = new Scanner(fileName);

            while(inputStream.hasNextLine()){
                String line= inputStream.nextLine();
                String[] values = line.split(",");
                // this adds the currently parsed line to the 2-dimensional string array
                lines.add(Arrays.asList(values));
            }

            inputStream.close();
        }catch (FileNotFoundException e) {
            System.err.println("csv file not found");
        }
        System.out.println(lines);
    }

}