package ru.fmtk.hlystov.examinationapp.services.auth;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import ru.fmtk.hlystov.examinationapp.Application;
import ru.fmtk.hlystov.examinationapp.TestConsole;
import ru.fmtk.hlystov.examinationapp.domain.User;
import ru.fmtk.hlystov.examinationapp.services.AppConfig;

import java.io.IOException;
import java.util.Locale;
import java.util.Optional;

import static org.junit.Assert.*;

public class UserAuthenticationTest {
    private AppConfig appConfig;
    private UserCredential userCredential;

    @Before
    public void initTests() {
        setAppConfig(new Locale("en", "En"));
        userCredential = new UserCredential("a", "b");
    }

    private void setAppConfig(@NotNull Locale locale) {
        Application app = new Application();
        appConfig = new AppConfig(locale, app.messageSource());
        appConfig.setBaseCSVResourceName("simple-exam-5-questions.csv");
    }

    @Test
    public void getUser() {
        UserAuthentication authentication = new UserAuthentication(appConfig);
        Optional<User> optionalUser = authentication.getUser(userCredential);
        assertTrue(optionalUser.isPresent());
    }

    @Test
    public void getEmptyUser() {
        UserAuthentication authentication = new UserAuthentication(appConfig);
        Optional<User> optionalUser = authentication.getUser(null);
        assertTrue(optionalUser.isEmpty());
    }
}