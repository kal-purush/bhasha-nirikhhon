package ru.otus.homework.hw12;

import java.io.*;
import java.util.*;

public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String myNameFiles = null;
        List<File> files = new ArrayList<>();
        String path = new File("").getAbsolutePath();

        File fileDirectory = new File(path);
        File[] fileList;
        if (fileDirectory.isDirectory()) {
            fileList = fileDirectory.listFiles();
            if (fileList != null) {
                for (var fileName : fileList) {
                    if (fileName.isFile()) {
                        files.add(fileName);
                        System.out.println(fileName.getName());
                    }
                }
            }
        }
        String name = scanner.nextLine();
        for (var file : files) {
            if (file.getName().equals(name)) {
                myNameFiles = file.getName();
                break;
            }
        }
        if (myNameFiles != null) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(myNameFiles)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
        if (myNameFiles != null) {
            try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(myNameFiles, true)))) {
                String text = scanner.nextLine();
                writer.write(text);
            } catch (
                    IOException e) {
                e.printStackTrace();
            }
        }
    }
}