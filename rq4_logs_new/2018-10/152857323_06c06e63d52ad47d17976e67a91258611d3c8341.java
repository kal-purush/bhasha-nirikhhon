package com.bank.model.service.validator;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class FileService {

    private static FileService instance;

    private FileService() {
    }

    public static FileService getInstance(){
        if (instance == null){
            instance = new FileService();
        }
        return instance;
    }

    public void write(String message, String filePath) throws IOException {
        PrintStream stream = new PrintStream(new FileOutputStream(filePath, true));
        stream.println(message);
    }
}