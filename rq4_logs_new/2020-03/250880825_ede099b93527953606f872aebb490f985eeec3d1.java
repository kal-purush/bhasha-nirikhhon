package com.company;

import io.Reader;

public class Main {

    public static void main(String[] args) throws Exception {
        Reader reader = new Reader();
        reader.start("./src/file.txt");
    }
}