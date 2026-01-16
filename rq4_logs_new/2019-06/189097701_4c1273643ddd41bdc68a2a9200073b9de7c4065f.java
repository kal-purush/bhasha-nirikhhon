package ru.fmtk.hlystov.examinationapp.domain.examination;

import org.junit.Before;
import org.junit.Test;
import ru.fmtk.hlystov.examinationapp.domain.examination.answer.NumericAnswer;
import ru.fmtk.hlystov.examinationapp.domain.examination.question.NumericQuestion;
import ru.fmtk.hlystov.examinationapp.domain.examination.question.Question;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class ExamImplTest {
    private List<String> options3;
    private List<Question> numericQuestionsList3;
    private NumericAnswer rightNumericAnswer;

    @Before
    public void setUp() {
        options3 = List.of("Option 1", "Option 2", "Option 3");
        rightNumericAnswer = new NumericAnswer(10.0);
        numericQuestionsList3 = List.of(
                new NumericQuestion("N1", options3, rightNumericAnswer),
                new NumericQuestion("N2", options3, rightNumericAnswer),
                new NumericQuestion("N3", options3, rightNumericAnswer));
    }

    @Test
    public void questionsNumber() {
        ExamImpl exam = new ExamImpl();
        assertEquals(0, exam.questionsNumber());
        exam.addQuestions(numericQuestionsList3);
        assertEquals(3, exam.questionsNumber());
    }

    @Test
    public void clear() {
        ExamImpl exam = new ExamImpl();
        exam.addQuestions(numericQuestionsList3);
        assertEquals(3, exam.questionsNumber());
        exam.clear();
        assertEquals(0, exam.questionsNumber());
    }

    @Test
    public void getQuestion() {
        ExamImpl exam = new ExamImpl();
        exam.addQuestions(numericQuestionsList3);
        Optional<Question> question = exam.getQuestion(0);
        assertTrue(question.isPresent());
        assertEquals("N1", question.get().getTitle());
        question = exam.getQuestion(1);
        assertTrue(question.isPresent());
        assertEquals("N2", question.get().getTitle());
        question = exam.getQuestion(2);
        assertTrue(question.isPresent());
        assertEquals("N3", question.get().getTitle());
        question = exam.getQuestion(-1);
        assertTrue(question.isEmpty());
        question = exam.getQuestion(3);
        assertTrue(question.isEmpty());
    }

    @Test
    public void addQuestion() {
        ExamImpl exam = new ExamImpl();
        exam.addQuestions(numericQuestionsList3);
        exam.addQuestion(new NumericQuestion("N4", options3, rightNumericAnswer));
        assertEquals(4, exam.questionsNumber());
        Optional<Question> question = exam.getQuestion(3);
        assertTrue(question.isPresent());
        assertEquals("N4", question.get().getTitle());
    }

    @Test
    public void addQuestions() {
        ExamImpl exam = new ExamImpl();
        exam.addQuestions(numericQuestionsList3);
        assertEquals(3, exam.questionsNumber());
    }

    @Test
    public void iterator() {
        ExamImpl exam = new ExamImpl();
        Iterator<Question> it = exam.iterator();
        assertNotNull(it);
        assertFalse(it.hasNext());
        exam.addQuestions(numericQuestionsList3);
        it = exam.iterator();
        assertNotNull(it);
        assertTrue(it.hasNext());
        Question question = it.next();
        assertNotNull(question);
    }

    @Test
    public void forEach() {
        ExamImpl exam = new ExamImpl();
        exam.addQuestions(numericQuestionsList3);
        ForEachTestStub forEachTestStub = new ForEachTestStub();
        exam.forEach(forEachTestStub::accept);
        assertEquals(3, forEachTestStub.getCnt());
    }

    private static class ForEachTestStub implements Consumer<Question> {
        private int cnt;

        public ForEachTestStub() {
            cnt = 0;
        }

        @Override
        public void accept(Question question) {
            cnt++;
        }

        public int getCnt() {
            return cnt;
        }
    }

    @Test
    public void spliterator() {
        ExamImpl exam = new ExamImpl();
        exam.addQuestions(numericQuestionsList3);
        Spliterator<Question> spliterator = exam.spliterator();
        assertNotNull(spliterator);
    }

    @Test
    public void getNumberToSuccess() {
        ExamImpl exam = new ExamImpl();
        exam.setRightAnswersToSuccess(3);
        assertEquals(3, exam.getNumberToSuccess());
    }

    @Test
    public void setRightAnswersToSuccess() {
        ExamImpl exam = new ExamImpl();
        exam.setRightAnswersToSuccess(2);
        assertEquals(2, exam.getNumberToSuccess());
    }
}