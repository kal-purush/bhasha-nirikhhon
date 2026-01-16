package ru.zhbert.docslinter.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class FileChecker {

    ArrayList<String> dict;
    int dictMaxLen = 0;

    public FileChecker(ArrayList<String> dict) {
        this.dict = dict;
        for (String word : dict) {
            if (word.length() > dictMaxLen) {
                dictMaxLen = word.length();
            }
        }
    }

    public void checkFile(File file) throws IOException {

        int currentLine = 0;
        String Format = "| %-" + dictMaxLen +"s | %-" + dictMaxLen + "s |%n";

        System.out.println("Checking file: " + file.getName());

        FileReader fr = new FileReader(file);
        BufferedReader reader = new BufferedReader(fr);
        String line = reader.readLine();
        while (line != null) {
            currentLine++;
            for (String word : dict) {
                if (line.toLowerCase().contains(word.toLowerCase())) {
                    int start = line.toLowerCase().indexOf(word.toLowerCase());
                    int end = start + word.length();
                    System.out.format(Format, word, line.substring(start, end));
                }
            }
            line = reader.readLine();
        }
    }

    public ArrayList<String> getDict() {
        return dict;
    }

    public void setDict(ArrayList<String> dict) {
        this.dict = dict;
    }
}