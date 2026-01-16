package ru.fmtk.hlystov.examinationapp.domain.examination.question;

import org.junit.Before;
import org.junit.Test;
import ru.fmtk.hlystov.examinationapp.domain.examination.answer.Answer;
import ru.fmtk.hlystov.examinationapp.domain.examination.answer.AnswerResult;
import ru.fmtk.hlystov.examinationapp.domain.examination.answer.NumericAnswer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class NumericQuestionTest {
    private String title;
    private List<String> emptyOptions;
    private Answer zeroAnswer;

    @Before
    public void initTests() {
        title = "Abcd 134 ¥";
        emptyOptions = new ArrayList<>();
        zeroAnswer = new NumericAnswer(0.0);
    }

    @Test
    public void getTitle() {
        NumericQuestion question = new NumericQuestion(title, emptyOptions, zeroAnswer);
        assertEquals(title, question.getTitle());
    }

    @Test
    public void getOptions() {
        List<String> options = Arrays.asList("123", "4 5 6", "ABC");
        NumericQuestion question = new NumericQuestion(title, options, zeroAnswer);
        assertThat(question.getOptions()).isEqualTo(options);
    }

    @Test
    public void getRightAnswer() {
        NumericQuestion question = new NumericQuestion(title, emptyOptions, zeroAnswer);
        assertTrue(zeroAnswer.isEquals(question.getRightAnswer()));
    }

    @Test
    public void checkRightAnswer() {
        NumericQuestion question = new NumericQuestion(title, emptyOptions, zeroAnswer);
        Answer newAnswer = new NumericAnswer(0.0);
        AnswerResult result = question.checkAnswers(newAnswer);
        assertThat(result).isNotNull();
        assertTrue(result.isRight());
    }

    @Test
    public void checkWrongAnswer() {
        NumericQuestion question = new NumericQuestion(title, emptyOptions, zeroAnswer);
        Answer newAnswer = new NumericAnswer(10.0);
        AnswerResult result = question.checkAnswers(newAnswer);
        assertThat(result).isNotNull();
        assertFalse(result.isRight());
    }

    @Test
    public void checkRightFractionalAnswerWithBasicPrecision() {
        Answer fractional = new NumericAnswer(1.0 / 3.0);
        Answer newAnswer = new NumericAnswer(0.33);
        NumericQuestion question = new NumericQuestion(title, emptyOptions, fractional);
        AnswerResult result = question.checkAnswers(newAnswer);
        assertThat(result).isNotNull();
        assertTrue(result.isRight());
    }

    @Test
    public void checkWrongBiggerFractionalAnswerWithBasicPrecision() {
        Answer fractional = new NumericAnswer(1.0 / 3.0);
        Answer newAnswer = new NumericAnswer(0.34);
        NumericQuestion question = new NumericQuestion(title, emptyOptions, fractional);
        AnswerResult result = question.checkAnswers(newAnswer);
        assertThat(result).isNotNull();
        assertFalse(result.isRight());
    }

    @Test
    public void checkWrongLowerFractionalAnswerWithBasicPrecision() {
        Answer fractional = new NumericAnswer(1.0 / 3.0);
        Answer newAnswer = new NumericAnswer(0.32);
        NumericQuestion question = new NumericQuestion(title, emptyOptions, fractional);
        AnswerResult result = question.checkAnswers(newAnswer);
        assertThat(result).isNotNull();
        assertFalse(result.isRight());
    }

    @Test
    public void checkRightFractionalAnswerWithLowPrecision() {
        Answer fractional = new NumericAnswer(1.0 / 3.0, 0.9);
        Answer newAnswer = new NumericAnswer(1.1);
        NumericQuestion question = new NumericQuestion(title, emptyOptions, fractional);
        AnswerResult result = question.checkAnswers(newAnswer);
        assertThat(result).isNotNull();
        assertTrue(result.isRight());
    }

    @Test
    public void checkWrongFractionalAnswerWithLowPrecision() {
        Answer fractional = new NumericAnswer(1.0 / 3.0, 0.9);
        Answer newAnswer = new NumericAnswer(1.3);
        NumericQuestion question = new NumericQuestion(title, emptyOptions, fractional);
        AnswerResult result = question.checkAnswers(newAnswer);
        assertThat(result).isNotNull();
        assertFalse(result.isRight());
    }

    @Test
    public void checkRightFractionalAnswerWithHighPrecision() {
        Answer fractional = new NumericAnswer(1.0 / 3.0, 0.0001);
        Answer newAnswer = new NumericAnswer(0.3333);
        NumericQuestion question = new NumericQuestion(title, emptyOptions, fractional);
        AnswerResult result = question.checkAnswers(newAnswer);
        assertThat(result).isNotNull();
        assertTrue(result.isRight());
    }

    @Test
    public void checkWrongFractionalAnswerWithHighPrecision() {
        Answer fractional = new NumericAnswer(1.0 / 3.0, 0.0001);
        Answer newAnswer = new NumericAnswer(0.33321);
        NumericQuestion question = new NumericQuestion(title, emptyOptions, fractional);
        AnswerResult result = question.checkAnswers(newAnswer);
        assertThat(result).isNotNull();
        assertFalse(result.isRight());
    }
}