package com.example.cbzcreator;

import java.io.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class CbzCreator {
    private String Dir;
    private String Info;
    private  String OutputFile;
    private String Name;
    public CbzCreator(String dir, String info,String name) {
        Dir = dir;
        Info = info;
    }
    public void Create() throws IOException {
        File dir=new File(this.Dir);
        File info=new File(Info);
        File out=new File(System.getProperty("user.home")+"/Desktop/"+this.Name+".cbz");
        ZipOutputStream stream = new ZipOutputStream(new FileOutputStream(out));
        ZipEntry entry=new ZipEntry("ComicContent/");
        stream.putNextEntry(entry);
    }
}