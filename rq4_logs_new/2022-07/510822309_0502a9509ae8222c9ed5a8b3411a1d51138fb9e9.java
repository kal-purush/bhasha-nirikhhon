package ru.sber.javaio.RemovingExtraSpaces;
import java.io.*;
import java.nio.file.Paths;
import java.nio.file.Files;

public class RemovingExtraSpaces {

    public static final String ORIGINAL_PATH = "JavaIO/src/ru/sber/javaio/RemovingExtraSpaces/Borodino.txt";
    public static final String REMOVING_PATH = "JavaIO/src/ru/sber/javaio/RemovingExtraSpaces/RemovingBorodino.txt";

    public static void main(String[] args) {

        try (BufferedReader fileInputStream = Files.newBufferedReader(Paths.get(ORIGINAL_PATH));
             BufferedWriter fileOutputStream = Files.newBufferedWriter(Paths.get(REMOVING_PATH))) {
            String b;
            while((b = fileInputStream.readLine()) != null){
                fileOutputStream.write(b.replaceAll("\\s+", " ") + "\n");
            }
            System.out.println("Запись в файл произведена");
        } catch(IOException exception) {
            System.out.println(exception.getMessage());
            exception.printStackTrace();
        }
    }
}