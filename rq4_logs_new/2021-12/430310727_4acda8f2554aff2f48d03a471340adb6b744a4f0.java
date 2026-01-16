package ru.otus.spring;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.Assert;
import ru.otus.spring.domain.Question;
import ru.otus.spring.service.TestingService;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DisplayName("Методы в TestingService должны ")
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class TestingServiceTests {

    @Autowired
    private TestingService testingService;


    @Test
    @DisplayName("Метод getActualScore должн возвращать набранные баллы при правильном ответе")
    public void testingServiceShouldReturnActualScoreRightAnswer() {
        Question question = new Question("1", "first question?", "a1");
        testingService.validateAnswer(question, "a1");
        int result = testingService.getActualScore();

        assertEquals(2, result);
    }

    @Test
    @DisplayName("Метод getActualScore должн возвращать набранные баллы при неправильном ответе")
    public void testingServiceShouldReturnActualScoreWrongAnswer() {
        Question question = new Question("1", "first question?", "a1");
        testingService.validateAnswer(question, "a2");
        int result = testingService.getActualScore();

        assertEquals(-1, result);
    }

    @Test
    @DisplayName("Метод validateAnswer в TestingService должн возвращать true на правильный ответ")
    public void testingServiceShouldValidateRightAnswer() {
        Question question = new Question("1", "first question?", "a1");
        boolean isCorrect = testingService.validateAnswer(question, "a1");
        Assert.isTrue(isCorrect, "Валидация ответа не правильная");
    }

    @Test
    @DisplayName("Метод validateAnswer в TestingService должн возвращать false на неправильный ответ")
    public void testingServiceShouldValidateWrongAnswer() {
        Question question = new Question("1", "first question?", "a1");
        boolean isCorrect = testingService.validateAnswer(question, "wrong");
        Assert.isTrue(!isCorrect, "Валидация ответа не правильная");
    }

    @Test
    @DisplayName("Метод getResult должн возвращать результаты тестирования при непройденном тестировании")
    public void testingServiceShouldReturnTestingResultsZeroAnswers() {
        String result = testingService.getResult();
        Assertions.assertEquals("Провал! ты набрал 0 / 6", result);
    }

    @Test
    @DisplayName("Метод getResult должн возвращать результаты тестирования при трех правильных ответах")
    public void testingServiceShouldReturnTestingResults() {
        Question question = new Question("1", "first question?", "a1");
        testingService.validateAnswer(question, "a1");
        testingService.validateAnswer(question, "a1");
        testingService.validateAnswer(question, "a1");
        String result = testingService.getResult();
        Assertions.assertEquals("Поздравляю! ты набрал 6 / 6", result);
    }


}