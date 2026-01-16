package ru.otus.spring.shell;

import lombok.RequiredArgsConstructor;
import org.springframework.shell.Availability;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;
import org.springframework.shell.standard.ShellOption;
import ru.otus.spring.service.LocalizationService;
import ru.otus.spring.service.TestingService;

@ShellComponent
@RequiredArgsConstructor
public class ShellApplication {

    private String studentName;
    private boolean finishedTesting;

    private final LocalizationService localService;
    private final TestingService testingService;

    @ShellMethod(value = "get student name", key = {"name", "login", "n", "l"})
    public void helloTesting(@ShellOption(defaultValue = "student") String studentName) {
        this.studentName = studentName;
        String hello = localService.getLocalString("strings.hello", studentName);
        System.out.println(hello);
    }

    @ShellMethod(value = "Start testing", key = {"start", "s", "t"})
    @ShellMethodAvailability(value = "isStudentLoggedIn")
    public void startTesting() {
        this.finishedTesting = testingService.testStudent();
    }

    @ShellMethod(value = "Get testing result", key = {"result", "r"})
    @ShellMethodAvailability(value = "isTestingFinished")
    public void getResults() {
        System.out.println(testingService.getResult());
    }


    private Availability isStudentLoggedIn() {
        String localString = localService.getLocalString("strings.login-first");
        return studentName == null ? Availability.unavailable(localString) : Availability.available();
    }

    private Availability isTestingFinished() {
        String localString = localService.getLocalString("strings.testing-first");
        return finishedTesting ? Availability.available() : Availability.unavailable(localString);
    }
}