package com.example.dsavisualizer;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class CSVIO {
    public static ArrayList<String[]> readCSV(String filename) throws FileNotFoundException {
        ArrayList<String[]> content = new ArrayList<>();
        Scanner reader = new Scanner(new FileReader(filename));
        while(reader.hasNext())
            content.add(reader.nextLine().split(", "));
        reader.close();
        return content;
    }
    public static void writeCSV(ArrayList<String[]> content, String filename) throws IOException {
        FileWriter writer = new FileWriter(filename);
        for(String[] row : content){
            for(int i = 0; i < row.length-1; i++)
                writer.append(row[i] + ", ");
            writer.append(row[row.length-1] + "\n");
        }
        writer.flush();
        writer.close();
    }
    public static void writeCSV(String[][] content, String filename) throws IOException {
        FileWriter writer = new FileWriter(filename);
        for(String[] row : content){
            for(int i = 0; i < row.length-1; i++)
                writer.append(row[i] + ", ");
            writer.append(row[row.length-1] + "\n");
        }
        writer.flush();
        writer.close();
    }
}