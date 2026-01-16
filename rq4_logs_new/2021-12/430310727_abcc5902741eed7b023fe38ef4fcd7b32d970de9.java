package dao;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import ru.otus.spring.domain.Question;
import ru.otus.spring.service.AnswerReader;
import ru.otus.spring.service.SimpleTestingService;
import ru.otus.spring.service.TestingService;

@SpringBootTest(classes = SimpleTestingService.class)
public class SimpleTestingServiceTest {

    @MockBean
    private AnswerReader answerReader;

    private TestingService testingService;

    @Test
    public void testingServiceShouldCountCorrect() {
        Question q1 = new Question("1", "first question?", "a1");

        testingService.askQuestion(q1);

        Assertions.assertEquals("", testingService.getResult());
    }

}