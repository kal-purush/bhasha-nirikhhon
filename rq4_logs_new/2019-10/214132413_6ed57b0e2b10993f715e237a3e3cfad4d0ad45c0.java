package ru.anovikov.learning.dao;

import ru.anovikov.learning.domain.Question;

import java.io.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;

public class QuestionDaoSimple implements QuestionDao{

    private String questionFile;

    public QuestionDaoSimple (String questionFile) {
        this.questionFile = questionFile;
    }

    public List<Question> findAll() {

        List<Question> lst = new ArrayList();

        ClassLoader classLoader = new Question().getClass().getClassLoader();

        File file = new File(classLoader.getResource(questionFile).getFile());

        try {
            Scanner scanner = new Scanner(file);
            while(scanner.hasNextLine()){
                Scanner valueScanner = new Scanner(scanner.nextLine());
                valueScanner.useDelimiter(";");
                Question qst = new Question();

                int i = 0;
                while (valueScanner.hasNext()) {
                    String data = valueScanner.next();
                    if (i == 0) {
                        qst.setText(data);
                    }
                    else if (i == 1) {
                        qst.setRightAnswer(Integer.parseInt(data));
                    }
                    else if (i == 2) {
                        qst.setAnswer1(data);
                    }
                    else if (i == 3) {
                        qst.setAnswer2(data);
                    }
                    else if (i == 4) {
                        qst.setAnswer3(data);
                    }
                    else if (i == 5) {
                        qst.setAnswer4(data);
                    }

                    i++;
                }

                lst.add(qst);
            }
            scanner.close();
        }
        catch (IOException e) {
            System.out.print("Error reading file.");
        }

        return  lst;
    }
}