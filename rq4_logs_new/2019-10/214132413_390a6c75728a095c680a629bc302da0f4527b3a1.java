package ru.anovikov.learning.otusstudenttest.service;

import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.context.SpringBootTest;
import ru.anovikov.learning.otusstudenttest.dao.QuestionDao;
import ru.anovikov.learning.otusstudenttest.domain.Question;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@SpringBootTest
class QuestionServiceTest {

    @InjectMocks
    QuestionServiceImpl questionService;

    @Mock
    QuestionDao dao;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void getAllQuestionsTest() {
        List<Question> list = new ArrayList<>();
        Question q1 = new Question("question1");
        q1.addAnswer("1");
        q1.addAnswer("2");
        q1.addAnswer("3");
        q1.addAnswer("4");
        q1.setRightAnswer(1);
        list.add(q1);

        Question q2 = new Question("question2");
        q2.addAnswer("1");
        q2.addAnswer("2");
        q2.addAnswer("3");
        q2.addAnswer("4");
        q2.setRightAnswer(1);
        list.add(q2);

        Question q3 = new Question("question3");
        q3.addAnswer("1");
        q3.addAnswer("2");
        q3.addAnswer("3");
        q3.addAnswer("4");
        q3.setRightAnswer(1);
        list.add(q3);

        when(dao.findAll()).thenReturn(list);

        //test
        List<Question> qstList = questionService.getAllQuestions();

        assertEquals(3, qstList.size());
    }
}