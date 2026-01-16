package ru.fmtk.hlystov.examinationapp.services.presenter;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import ru.fmtk.hlystov.examinationapp.Application;
import ru.fmtk.hlystov.examinationapp.domain.User;
import ru.fmtk.hlystov.examinationapp.domain.UserImpl;
import ru.fmtk.hlystov.examinationapp.domain.examination.answer.Answer;
import ru.fmtk.hlystov.examinationapp.domain.examination.answer.NumericAnswer;
import ru.fmtk.hlystov.examinationapp.domain.examination.question.NumericQuestion;
import ru.fmtk.hlystov.examinationapp.domain.examination.question.Question;
import ru.fmtk.hlystov.examinationapp.services.AppConfig;
import ru.fmtk.hlystov.examinationapp.services.auth.UserAuthentification;
import ru.fmtk.hlystov.examinationapp.services.converter.StringsToAnswerConverter;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Scanner;

import static org.junit.Assert.*;

public class ConsolePresenterTest {
    private PipedOutputStream outToInput;
    private ByteArrayOutputStream outputBytes;
    private InputStream inputStream;
    private PrintStream printStream;
    private AppConfig appConfig;
    private User user;
    private UserAuthStub userAuth;
    private StringsToAnswerConverter answerConverter;
    private ConsolePresenter consolePresenter;

    @Before
    public void initTest() throws IOException {
        Application app = new Application();
        appConfig = new AppConfig(new Locale("en", "En"),
                "simple-exam-5-questions.csv",
                app.messageSource());
        outputBytes = new ByteArrayOutputStream();
        printStream = new PrintStream(outputBytes);
        outToInput = new PipedOutputStream();
        inputStream = new PipedInputStream(outToInput);
        user = new UserImpl("a", "b");
        userAuth = new UserAuthStub(user);
        answerConverter = new StringsToAnswerConverter();
        consolePresenter = new ConsolePresenter(appConfig, userAuth, answerConverter,
                inputStream, printStream);
    }

    @Test
    public void showMessage() {
        String messageEn = "Test message 1234!";
        consolePresenter.showMessage(messageEn);
        printStream.flush();
        Scanner sc = new Scanner(new ByteArrayInputStream(outputBytes.toByteArray()));
        assertEquals(messageEn, sc.nextLine());
    }

    @Test
    public void readString() throws IOException {
        String messageEn = "Test message 1234!";
        outToInput.write((messageEn + '\n').getBytes());
        String newString = consolePresenter.readString();
        assertEquals(messageEn, newString);
    }

    @Test
    public void getUserNotNull() {
        User gotUser = consolePresenter.getUser();
        assertEquals(user, gotUser);
    }

    @Test
    public void askNumericQuestionWithRightAnswer() throws IOException {
        double rightNumber = 5.333;
        String title = "Numeric title 1";
        List<String> options = new ArrayList<>();
        NumericAnswer rightAnswer = new NumericAnswer(rightNumber);
        int number = 0;
        Question question = new NumericQuestion(title, options, rightAnswer);
        outToInput.write((Double.toString(rightNumber) + '\n').getBytes());
        outToInput.flush();
        Answer newAnswer = consolePresenter.askQuestion(number, question);
        assertTrue(rightAnswer.isEquals(newAnswer));
    }

    @Test
    public void askNumericQuestionWithWrongAnswer() throws IOException {
        double rightNumber = 5.333;
        String title = "Numeric title 1";
        List<String> options = new ArrayList<>();
        NumericAnswer rightAnswer = new NumericAnswer(rightNumber);
        int number = 0;
        Question question = new NumericQuestion(title, options, rightAnswer);
        outToInput.write((Double.toString(rightNumber * 10.0) + '\n').getBytes());
        outToInput.flush();
        Answer newAnswer = consolePresenter.askQuestion(number, question);
        assertFalse(rightAnswer.isEquals(newAnswer));
    }

    @Test
    public void showStatistics() {
    }

    @Test
    public void showExamStart() {
    }

    @Test
    public void showGreetengs() {
    }

    @Test
    public void showUserNeeded() {
    }

    @Test
    public void showGoodBy() {
    }

    @Test
    public void showAnswerResult() {
    }

    @Test
    public void showExamResult() {
    }

    @Test
    public void getResString() {
    }

    public class UserAuthStub implements UserAuthentification {
        @NotNull
        final
        User user;

        public UserAuthStub(@NotNull User user) {
            this.user = user;
        }

        @Override
        @NotNull
        public User getUser() {
            return user;
        }
    }
}