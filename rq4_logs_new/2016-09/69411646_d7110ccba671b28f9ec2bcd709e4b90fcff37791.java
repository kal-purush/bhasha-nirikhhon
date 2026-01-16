package Controler;

import Moduler.Link;
import java.util.Scanner;
import java.io.*;

public class FileOperations {

    public Scanner sc;
    private String filepath;
    private String word = "";
    private Link dataItem;

    public FileOperations(String path) {

        filepath = path;

        try {

            sc = new Scanner(new File(filepath));

        } catch (FileNotFoundException e) {

            System.out.println(e.getMessage());
        }

    }

    public int wordcount() {

        int count = 0;

        while (sc.hasNext()) {

            word = sc.next();

            count++;

        }
        return count;
    }

    public HashTable fillData(HashTable hash) {
        
        try {

            sc = new Scanner(new File(filepath));

        } catch (FileNotFoundException e) {

            System.out.println(e.getMessage());
        }

        while (sc.hasNext()) {
            dataItem = new Link(sc.next().replaceAll("[^\\w\\s]","").toLowerCase());
            hash.insert(dataItem);
        }
        return hash;
    }
}