package com.example.cbzcreator;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipFolderUtil {

    static private final int BUFFER = 1024;
    private List<File> FileList = new ArrayList<File>();
    private final File FolderToZip;
    private final File ZipOut;

    public ZipFolderUtil(File folderToZip, File zipOut) {
        this.FolderToZip = folderToZip;
        this.ZipOut = zipOut;
        this.FileList = getFileList(this.FolderToZip);
    }

    public void pack() {
        this.zipFiles(this.FileList);
    }

    private void zipFiles(List<File> fileList){
        try {
            FileOutputStream fos = new FileOutputStream(this.ZipOut);
            ZipOutputStream zos = new ZipOutputStream(fos);

            for(File file : fileList) {
                if(file.isDirectory()) {
                    ZipEntry ze = new ZipEntry(getFileName(file.toString())+"/");
                    zos.putNextEntry(ze);
                    zos.closeEntry();
                }
                else {
                    FileInputStream fis = new FileInputStream(file);
                    BufferedInputStream bis = new BufferedInputStream(fis, BUFFER);
                    ZipEntry ze = new ZipEntry(getFileName(file.toString()));
                    zos.putNextEntry(ze);

                    byte data[] = new byte[BUFFER];
                    int count;

                    while((count = bis.read(data, 0, BUFFER)) != -1) {
                        zos.write(data, 0, count);
                    }

                    bis.close();
                    zos.closeEntry();
                }
            }
            zos.close();
        }
        catch(IOException ioExp) {
            System.out.println("Error while zipping " + ioExp.getMessage());
            ioExp.printStackTrace();
        }
    }

    private List<File> getFileList(File source){
        if(source.isFile()) {
            FileList.add(source);
        }
        else if(source.isDirectory()) {
            String[] subList = source.list();
            if(subList.length == 0){
                FileList.add(new File(source.getAbsolutePath()));
            }
            for(String child : subList){
                getFileList(new File(source, child));
            }
        }
        return FileList;
    }

    private String getFileName(String filePath){
        return filePath.substring(FolderToZip.getPath().length() + 1, filePath.length());
    }
}