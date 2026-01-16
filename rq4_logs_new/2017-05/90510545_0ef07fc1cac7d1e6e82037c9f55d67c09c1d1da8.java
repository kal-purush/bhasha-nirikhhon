package com.test;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

/**
 * Created by krishna1bhat on 5/7/17.
 */
public class Main {
    public static void main(String[] args) {
        //FileNotFoundException -- by compiler -- checked exception
        PrintWriter pr = null;
        try {
            pr = new PrintWriter("myFile.txt");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        pr.println("This is test");

        //unchecked exception - compiler ignores these types becoz happens rarely...
        System.out.println(100/0);
    }
}