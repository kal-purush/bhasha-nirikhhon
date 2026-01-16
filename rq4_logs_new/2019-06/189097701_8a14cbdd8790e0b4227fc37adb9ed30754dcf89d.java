package ru.fmtk.hlystov.examinationapp.services.converter;

import org.junit.Before;
import org.junit.Test;
import ru.fmtk.hlystov.examinationapp.domain.examination.answer.Answer;
import ru.fmtk.hlystov.examinationapp.domain.examination.answer.NumericAnswer;
import ru.fmtk.hlystov.examinationapp.domain.examination.answer.OptionsAnswer;
import ru.fmtk.hlystov.examinationapp.domain.examination.answer.SingleAnswer;
import ru.fmtk.hlystov.examinationapp.domain.examination.question.NumericQuestion;
import ru.fmtk.hlystov.examinationapp.domain.examination.question.OptionsQuestion;
import ru.fmtk.hlystov.examinationapp.domain.examination.question.SingleQuestion;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StringsToAnswerConverterTest {
    private StringsToAnswerConverter converter;

    @Before
    public void initTests() {
        converter = new StringsToAnswerConverter();
    }

    @Test
    public void convertNumericAnswer() {
        double number = 800.139;
        List<String> answers = Arrays.asList(new String[]{Double.toString(number)});
        Answer answer = converter.convertAnswer(NumericQuestion.class, answers);
        assertTrue((new NumericAnswer(number)).isEquals(answer));
    }

    @Test
    public void convertRightSingleAnswer() {
        int value = 1;
        List<String> answers = Arrays.asList(new String[]{Integer.toString(value), "2", "3"});
        Answer answer = converter.convertAnswer(SingleQuestion.class, answers);
        assertTrue(new SingleAnswer(value).isEquals(answer));
    }

    @Test
    public void convertWrongSingleAnswer() {
        int value = 1;
        List<String> answers = Arrays.asList(new String[]{"2", Integer.toString(value), "3"});
        Answer answer = converter.convertAnswer(SingleQuestion.class, answers);
        assertFalse(new SingleAnswer(value).isEquals(answer));
    }

    @Test
    public void convertStraightOrderOptionsAnswer() {
        List<Integer> intAnswers = Arrays.asList(new Integer[]{1, 2, 3, 4});
        List<String> answers = Arrays.asList(new String[]{"1", "2", "3", "4"});
        Answer answer = converter.convertAnswer(OptionsQuestion.class, answers);
        assertTrue(new OptionsAnswer(intAnswers).isEquals(answer));
    }

    @Test
    public void convertRandomOrderOptionsAnswer() {
        List<Integer> intAnswers = Arrays.asList(new Integer[]{2, 1, 3});
        List<String> answers = Arrays.asList(new String[]{"3", "2", "1"});
        Answer answer = converter.convertAnswer(OptionsQuestion.class, answers);
        assertTrue(new OptionsAnswer(intAnswers).isEquals(answer));
    }

    @Test
    public void convertRepetedOptionsAnswer() {
        List<Integer> intAnswers = Arrays.asList(new Integer[]{1, 2, 1});
        List<String> answers = Arrays.asList(new String[]{"2", "1", "2"});
        Answer answer = converter.convertAnswer(OptionsQuestion.class, answers);
        assertTrue(new OptionsAnswer(intAnswers).isEquals(answer));
    }

    @Test
    public void convertWrondOptionsAnswer() {
        List<Integer> intAnswers = Arrays.asList(new Integer[]{1, 2, 3, 4});
        List<String> answers = Arrays.asList(new String[]{"5", "2", "3", "4"});
        Answer answer = converter.convertAnswer(OptionsQuestion.class, answers);
        assertFalse(new OptionsAnswer(intAnswers).isEquals(answer));
    }

    @Test
    public void addConverter() {
    }
}