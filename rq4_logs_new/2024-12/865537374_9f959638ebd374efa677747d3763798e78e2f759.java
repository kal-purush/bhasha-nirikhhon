package otus.spring.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import otus.spring.config.TestConfig;
import otus.spring.domain.Student;
import otus.spring.domain.TestResult;

import static org.mockito.Mockito.*;
import static otus.spring.enums.Const.CONSOLE_TEXT_COLOR_RESET;

@ExtendWith(MockitoExtension.class)
public class ResultServiceImplTest {

    @Mock
    private TestConfig testConfig;

    @Mock
    private IOService ioService;

    @InjectMocks
    private ResultServiceImpl resultService;

    @BeforeEach
    void setUp() {
        when(testConfig.getRightAnswersCountToPass()).thenReturn(3);
    }

    @Test
    void testShowResultWhenStudentPassesThenPrintPassedMessage() {
        Student student = new Student("John", "Doe");
        TestResult testResult = new TestResult(student);
        testResult.setRightAnswersCount(3);

        resultService.showResult(testResult);

        verify(ioService).printLine("");
        verify(ioService).printLine("ResultService.test.results");
        verify(ioService).printFormattedColoredLine("ResultService.student", CONSOLE_TEXT_COLOR_RESET, student.getFullName());
        verify(ioService).printFormattedColoredLine("ResultService.answered.questions.count", CONSOLE_TEXT_COLOR_RESET, String.valueOf(testResult.getAnsweredQuestions().size()));
        verify(ioService).printFormattedColoredLine("ResultService.right.answers.count", CONSOLE_TEXT_COLOR_RESET, String.valueOf(testResult.getRightAnswersCount()));
        verify(ioService).printLine("ResultService.passed.test");
        verify(ioService, never()).printLine("ResultService.fail.test");
    }

    @Test
    void testShowResultWhenStudentFailsThenPrintFailMessage() {
        Student student = new Student("Jane", "Doe");
        TestResult testResult = new TestResult(student);
        testResult.setRightAnswersCount(2);

        resultService.showResult(testResult);

        verify(ioService).printLine("");
        verify(ioService).printLine("ResultService.test.results");
        verify(ioService).printFormattedColoredLine("ResultService.student", CONSOLE_TEXT_COLOR_RESET, student.getFullName());
        verify(ioService).printFormattedColoredLine("ResultService.answered.questions.count", CONSOLE_TEXT_COLOR_RESET, String.valueOf(testResult.getAnsweredQuestions().size()));
        verify(ioService).printFormattedColoredLine("ResultService.right.answers.count", CONSOLE_TEXT_COLOR_RESET, String.valueOf(testResult.getRightAnswersCount()));
        verify(ioService).printLine("ResultService.fail.test");
        verify(ioService, never()).printLine("ResultService.passed.test");
    }
}