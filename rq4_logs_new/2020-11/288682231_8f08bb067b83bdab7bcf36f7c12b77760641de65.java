package ru.moore;

import java.io.*;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Log {

    private String filePath = "log.txt";
    private File file;

    public Log() {
        file = new File(filePath);
    }

    public void writeFile(String message) {
        String timestamp = DateFormat.getInstance().format(new Date());
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            long fileLength = file.length();
            randomAccessFile.seek(fileLength);

            randomAccessFile.write(timestamp.getBytes());
            randomAccessFile.write(System.lineSeparator().getBytes());
            randomAccessFile.write(message.getBytes());
            randomAccessFile.write(System.lineSeparator().getBytes());
            randomAccessFile.write(System.lineSeparator().getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String readFile() {
        int maxLines = 100;
        int readLines = 0;
        StringBuilder builder = new StringBuilder();
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            long fileLength = file.length() - 1;
            randomAccessFile.seek(fileLength);

            for (long pointer = fileLength; pointer >= 0; pointer--) {
                randomAccessFile.seek(pointer);
                char c;
                c = (char) randomAccessFile.read();
                if (c == '\n') {
                    readLines++;
                    if (readLines == maxLines)
                        break;
                }

                builder.append(c);
                fileLength = fileLength - pointer;
            }
            builder.reverse();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return builder.toString();
    }
}