package ru.innopolis.attestations.attestation01.models;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.innopolis.attestations.attestation01.exceptions.ValidationException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UserTest {

    User user;

    @BeforeEach
    void setUp() {
        user = new User();
    }

    @Test
    void setValidLogin() {
        var login = "login123_";
        user.setLogin(login);
        assertEquals(login, user.getLogin());
    }

    @ParameterizedTest
    @MethodSource
    void setInvalidLogin(String invalidLogin) {
        assertThrows(ValidationException.class, () -> user.setLogin(invalidLogin));
    }

    static Stream<String> setInvalidLogin() {
        var maxLengthExceedLogin = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_";
        var onlyLettersLogin = "login";
        var onlyDigitsLogin = "1234567890";
        var loginWoUnderscore = "login123";
        return Stream.of(
                null,
                maxLengthExceedLogin,
                onlyDigitsLogin,
                onlyLettersLogin,
                loginWoUnderscore
        );
    }

    @Test
    void setPasswords() {
        var password = "password123_";
        user.setPasswords(password, password);
        assertEquals(password, user.getPassword());
        assertEquals(password, user.getConfirmPassword());
    }

    @ParameterizedTest
    @MethodSource
    void setInvalidPasswords(String password, String confirmPassword) {
        assertThrows(ValidationException.class, () -> user.setPasswords(password, confirmPassword));
    }

    static Stream<Arguments> setInvalidPasswords() {
        var validPassword = "password123_";
        var maxLengthExceedPassword = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_";
        var onlyLettersPassword = "password";
        var onlyDigitsPassword = "1234567890";
        var PasswordWoUnderscore = "password123";
        String[] invalidPasswords = {maxLengthExceedPassword, onlyLettersPassword, onlyDigitsPassword, PasswordWoUnderscore};
        List<Arguments> invalidPasswordsArguments = new ArrayList<>();
        for (String invalidPassword : invalidPasswords) {
            invalidPasswordsArguments.add(Arguments.of(validPassword, invalidPassword));
            invalidPasswordsArguments.add(Arguments.of(invalidPassword, validPassword));
        }
        return invalidPasswordsArguments.stream();
    }

    @Test
    void setDifferentPasswords() {
        assertThrows(ValidationException.class, () -> user.setPasswords("password123_", "_password123"));
    }

    @Test
    void setLastName() {
        var lastName = "Маслов";
        user.setLastName(lastName);
        assertEquals(lastName, user.getLastName());
    }

    @ParameterizedTest
    @MethodSource
    void setInvalidLastName(String invalidLastName) {
        assertThrows(ValidationException.class, () -> user.setLastName(invalidLastName));
    }

    static Stream<String> setInvalidLastName() {
        return Stream.of(
                null,
                "Маслов123"
        );
    }

    @Test
    void setFirstName() {
        var firstName = "Артур";
        user.setFirstName(firstName);
        assertEquals(firstName, user.getFirstName());
    }

    @ParameterizedTest
    @MethodSource
    void setInvalidFirstName(String invalidFirstName) {
        assertThrows(ValidationException.class, () -> user.setFirstName(invalidFirstName));
    }

    static Stream<String> setInvalidFirstName() {
        return Stream.of(
                null,
                "Джон123"
        );
    }

    @ParameterizedTest
    @MethodSource
    void setMiddleName(String middleName) {
        user.setMiddleName(middleName);
        assertEquals(middleName, user.getMiddleName());
    }

    static Stream<String> setMiddleName() {
        return Stream.of(null, "Иванович");
    }

    @Test
    void setInvalidMiddleName() {
        assertThrows(ValidationException.class, () -> user.setMiddleName("Иванович123"));
    }

    @Test
    void setAge() {
        int age = 35;
        user.setAge(age);
        assertEquals(age, user.getAge());
    }

    @Test
    void setWorker() {
        boolean isWorker = false;
        user.setWorker(isWorker);
        assertEquals(isWorker, user.isWorker());
    }

    @Test
    void setId() {
        var id = "id123_";
        user.setId(id);
        assertEquals(id, user.getId());
    }

    @ParameterizedTest
    @MethodSource
    void setInvalidId(String invalidId) {
        assertThrows(ValidationException.class, () -> user.setId(invalidId));
    }

    static Stream<String> setInvalidId() {
        var onlyDigitsId = "1234567890";
        var onlyLettersId = "id";
        return Stream.of(
                null,
                onlyDigitsId,
                onlyLettersId
        );
    }

}