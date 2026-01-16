package com.example.musicdownloader.Utilities;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ListofFiles {
    public ArrayList<String> main() throws IOException {
        //Going to create the file object
        File directoryPath = new File("/home/dude/Documents/Music/");

        // List of all files
        String contents[] = directoryPath.list();

        System.out.println(contents);
        ArrayList<String> fileList = new ArrayList<String>();

        for (int i = 0; i<contents.length ; i++)
        {
            fileList.add(contents[i]);

        }

        return fileList;
    }
}