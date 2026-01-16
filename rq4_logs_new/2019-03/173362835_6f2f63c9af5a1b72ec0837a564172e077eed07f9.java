package ru.otus.homework1.dao;

import com.opencsv.CSVReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CsvQuizDao implements QuizDao {
    private static final String CSV_FILE = "questions.csv";

    @Override
    public List<String> getQuestions() {
        CSVReader csvReader = null;
        List<String> res = new ArrayList<>();
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            File file = new File(classLoader.getResource(CSV_FILE).getFile());
            csvReader = new CSVReader(new FileReader(file));
            res = csvReader.readAll().
                    stream().
                    map(line -> line[0]).
                    collect(Collectors.toList());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (csvReader != null) {
                try {
                    csvReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return res;
    }
}