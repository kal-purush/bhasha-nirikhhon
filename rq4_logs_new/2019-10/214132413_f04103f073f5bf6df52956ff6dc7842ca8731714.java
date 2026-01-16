package ru.anovikov.learning;

import ru.anovikov.learning.domain.Question;
import ru.anovikov.learning.service.QuestionService;
import ru.anovikov.learning.service.TestingService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class TestingRunner {

    String userName;
    String userLastName;
    int correctAnsw;

    public void StartTesting (TestingService testingService) {

        userName = testingService.readUserName();
        userLastName = testingService.readUserLastName();
        correctAnsw = 0;

        Question q = testingService.getNextQuestion();
        int questionCount = 0;
        while (q != null) {

            testingService.printQuestion(q);

            if (testingService.readTestAnswer() == q.getRightAnswer()) {
                correctAnsw++;
            }

            q = testingService.getNextQuestion();
            questionCount++;
        }

        System.out.println("Result: " + correctAnsw + " out " + questionCount);
    }
}