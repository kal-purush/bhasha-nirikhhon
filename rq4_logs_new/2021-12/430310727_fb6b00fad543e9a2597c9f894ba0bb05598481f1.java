package ru.otus.spring.dao;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Component
public class CsvFileReader implements FileReader {

    private List<String> stringDataList;
    private String sourceFile;

    public CsvFileReader(@Value("questions.csv") String sourceFile) {
        this.sourceFile = sourceFile;
    }


    @Override
    public List<String> reedCsvFile() {
        stringDataList = new ArrayList<>();
        try (BufferedReader reader = initReader()) {
            String line = reader.readLine();
            while (line != null) {
                stringDataList.add(line);
                line = reader.readLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("file path error", e);
        }
        return stringDataList;
    }

    @Override
    public BufferedReader initReader() {

        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream(sourceFile);
        InputStreamReader streamReader = new InputStreamReader(is, StandardCharsets.UTF_8);

        return new BufferedReader(streamReader);
    }
}