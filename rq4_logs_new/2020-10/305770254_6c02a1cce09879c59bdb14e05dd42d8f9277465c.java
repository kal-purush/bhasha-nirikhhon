package com.example.filemanager;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileUtils {
    public static List<ListItem> convertToListItems(File[] files){
        List<ListItem> listFiles = Collections.emptyList();
        if(files!=null){
            listFiles = new ArrayList<>();
            for(File file: files){
                listFiles.add(convertToListItem(file));
            }
        }
        return listFiles;
    }
    public static ListItem convertToListItem(File file){
        ListItem listItem = new ListItem();
        listItem.setText(file.getName());
        return listItem;
    }
}