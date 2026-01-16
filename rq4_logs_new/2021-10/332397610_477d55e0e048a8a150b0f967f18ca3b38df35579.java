package ru.job4j.search;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SearchDirectory {
    public static void main(String[] args) {
        String dir = ("/home/");
        File file = new File(dir);
        File[] files = file.listFiles();
        for (File f : files) {
            System.out.println(f.getName());
        }

    }
}