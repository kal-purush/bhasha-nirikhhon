package com.training.cleannames;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.Scanner;


public class Merge {
    public static void main(String[] args) {
//        String s = "testing/SimpleTextFile.txt";
//        File f = new File(s);
        new Merge().readAndWrite();
    }

    public void readAndWrite(){
        //read the input path
        Scanner scanner = new Scanner(System.in);
        String s = scanner.nextLine();
        copy(new File(s),true);

    }

    public void copy(File fileToLoadOrig, boolean withDuplicate){
        //Let us get the current Date
        Date date = new Date();
        LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        int year  = localDate.getYear();
        int month = localDate.getMonthValue();
        int day   = localDate.getDayOfMonth();

        String destDirectory = year + "-" + month + "-" + day;

        //check if directory is created

        File destFile = new File("./testing/dest/"+destDirectory);
        System.out.println("the file path:"+ destFile.getAbsolutePath());

        if(! destFile.exists()){
            System.out.println("creating a new directory with path:"+ destFile.getAbsolutePath());
            boolean created = destFile.mkdirs();
            System.out.println("is file created:"+created);
        }
        System.out.println("File is created");
        //Great, now let us check if the file already in the dirctory

        File[] files = destFile.listFiles();
        boolean duplicateFound = false;


        File[] sourceFiles = fileToLoadOrig.listFiles();

        for(File fileToLoad: sourceFiles) {
            for (File file : files) {
                if (file.getName().equals(fileToLoad.getName())) {
                    if (withDuplicate) {
                        int maxSeqNum = 0;
                        //Let us find the last sequence of the file
                        for (File fileForDuplicate : files) {
                            String[] fileParts = fileForDuplicate.getName().split("_");
                            if (fileParts.length == 2) {
                                //got a split
                                if (fileParts[0].equals(file.getName())) {
                                    maxSeqNum = Math.max(maxSeqNum, Integer.parseInt(fileParts[1]));
                                }
                            }
                        }
                        String newFilePath = fileToLoad.getName() + "_" + (maxSeqNum + 1);
                        try {
                            FileUtils.copyFile(fileToLoad, new File(destFile.getAbsoluteFile() + "/" + newFilePath));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else {
                        // let us delete the file and replace it
                        file.delete();
                        try {
                            FileUtils.copyFile(fileToLoad, new File(destFile.getAbsoluteFile() + "/" + fileToLoad.getName()));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                } else {
                    try {
                        FileUtils.copyFile(fileToLoad, new File(destFile.getAbsoluteFile() + "/" + fileToLoad.getName()));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                FileUtils.copyFile(fileToLoad, new File(destFile.getAbsoluteFile() + "/" + fileToLoad.getName()));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public void copy2(File fileToLoadOrig, boolean withDuplicate){
        //Let us get the current Date
        Date date = new Date();
        LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        int year  = localDate.getYear();
        int month = localDate.getMonthValue();
        int day   = localDate.getDayOfMonth();

        String destDirectory = year + "-" + month + "-" + day;

        //check if directory is created

        File destFile = new File("./testing/dest/"+destDirectory);
        System.out.println("the file path:"+ destFile.getAbsolutePath());

        if(! destFile.exists()){
            System.out.println("creating a new directory with path:"+ destFile.getAbsolutePath());
            boolean created = destFile.mkdirs();
            System.out.println("is file created:"+created);
        }
        System.out.println("File is created");
        //Great, now let us check if the file already in the dirctory

        File[] files = destFile.listFiles();
        boolean duplicateFound = false;


        File[] sourceFiles = fileToLoadOrig.listFiles();

        for(File fileToLoad: sourceFiles) {
            for (File file : files) {
                if (file.getName().equals(fileToLoad.getName())) {
                    if (withDuplicate) {
                        int maxSeqNum = 0;
                        //Let us find the last sequence of the file
                        for (File fileForDuplicate : files) {
                            String[] fileParts = fileForDuplicate.getName().split("_");
                            if (fileParts.length == 2) {
                                //got a split
                                if (fileParts[0].equals(file.getName())) {
                                    maxSeqNum = Math.max(maxSeqNum, Integer.parseInt(fileParts[1]));
                                }
                            }
                        }
                        String newFilePath = fileToLoad.getName() + "_" + (maxSeqNum + 1);
                        try {
                            FileUtils.copyFile(fileToLoad, new File(destFile.getAbsoluteFile() + "/" + newFilePath));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else {
                        // let us delete the file and replace it
                        file.delete();
                        try {
                            FileUtils.copyFile(fileToLoad, new File(destFile.getAbsoluteFile() + "/" + fileToLoad.getName()));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                } else {
                    try {
                        FileUtils.copyFile(fileToLoad, new File(destFile.getAbsoluteFile() + "/" + fileToLoad.getName()));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                FileUtils.copyFile(fileToLoad, new File(destFile.getAbsoluteFile() + "/" + fileToLoad.getName()));
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }
}