package com.triviamachine.triviamachine;

import com.triviamachine.triviamachine.database.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DatabaseEntityTest {
    @Autowired
    DateFormat dateFormat;

    @Test
    public void testAdminUser() {
        AdminUser test = new AdminUser();
        assertNotNull(test);

        test.setPassword("THING");
        test.setUsername("ME");

        assertEquals("ME", test.getUsername());
        assertEquals("THING", test.getPassword());
        assertTrue(test.isAccountNonExpired());
        assertTrue(test.isAccountNonLocked());
        assertTrue(test.isCredentialsNonExpired());
        assertTrue(test.isEnabled());
    }

    @Test
    public void testQuestion() {
        Question test = new Question();

        test.setQuestionText("QUESTION");
        test.setAnswerOne("ONE");
        test.setAnswerTwo("TWO");
        test.setAnswerThree("THREE");
        test.setAnswerFour("FOUR");
        test.setCorrectAnswer((byte) 2);

        assertEquals("QUESTION", test.getQuestionText());
        assertEquals("ONE", test.getAnswerOne());
        assertEquals("TWO", test.getAnswerTwo());
        assertEquals("THREE", test.getAnswerThree());
        assertEquals("FOUR", test.getAnswerFour());
        assertEquals((byte) 2, test.getCorrectAnswer());
    }

    @Test
    public void testQuestionSchedule() throws ParseException {
        Question testQuestion = new Question();
        Results testResults = new Results();

        QuestionSchedule test = new QuestionSchedule();
        assertNotNull(test);

        test.setQuestion(testQuestion);
        test.setResults(testResults);
        test.setDate(dateFormat.parse(dateFormat.format(new Date())));

        assertSame(testQuestion, test.getQuestion());
        assertSame(testResults, test.getResults());
        assertEquals(dateFormat.parse(dateFormat.format(new Date())), test.getDate());
        assertEquals(dateFormat.format(new Date()), test.getDateToString());
    }

    @Test
    public void testResults() {
        Results test = new Results();

        assertNotNull(test);

        Question testQuestion = new Question();
        testQuestion.setCorrectAnswer((byte) (0));
        QuestionSchedule testSchedule = new QuestionSchedule();

        test.setQuestion(testQuestion);

        test.setSchedule(testSchedule);

        assertSame(testQuestion, test.getQuestion());
        assertSame(testSchedule, test.getSchedule());

        test.setAnswerOneVotes(1);
        test.setAnswerTwoVotes(2);
        test.setAnswerThreeVotes(3);
        test.setAnswerFourVotes(4);

        assertEquals(1, test.getAnswerOneVotes());
        assertEquals(2, test.getAnswerTwoVotes());
        assertEquals(3, test.getAnswerThreeVotes());
        assertEquals(4, test.getAnswerFourVotes());
        assertEquals(0, test.getId());

        ResultsJsonModel testModel = test.convertToJson();

        assertEquals(1, test.getAnswerOneVotes());
        assertEquals(2, test.getAnswerTwoVotes());
        assertEquals(3, test.getAnswerThreeVotes());
        assertEquals(4, test.getAnswerFourVotes());
        assertEquals(0, test.getQuestion().getCorrectAnswer());
    }
}