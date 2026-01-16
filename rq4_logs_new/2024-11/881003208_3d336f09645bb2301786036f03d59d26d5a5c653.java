package ru.yandex.practicum.filmorate.utility;

import org.junit.jupiter.api.Test;
import ru.yandex.practicum.filmorate.exception.ValidationException;
import ru.yandex.practicum.filmorate.model.Film;
import ru.yandex.practicum.filmorate.model.User;

import java.time.Duration;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class ValidatorTest {
    
    @Test
    void shouldNotValidateEmptyNameFilm() {
        Film film = new Film(0, "", "description", LocalDate.now(), Duration.ofHours(1));
        assertThrows(ValidationException.class, () -> Validator.validateFilm(film));

        film.setName(null);
        assertThrows(ValidationException.class, () -> Validator.validateFilm(film));
    }

    @Test
    void shouldNotValidateLongDescriptionFilm() {
        Film film = new Film(0, "name", "*".repeat(201), LocalDate.now(), Duration.ofHours(1));
        assertThrows(ValidationException.class, () -> Validator.validateFilm(film));
    }

    @Test
    void shouldValidateLen200DescriptionFilm() {
        Film film = new Film(0, "name", "*".repeat(200), LocalDate.now(), Duration.ofHours(1));
        assertDoesNotThrow(() -> Validator.validateFilm(film));
    }

    @Test
    void shouldValidateBlankDescriptionFilm() {
        Film film = new Film(0, "name", "", LocalDate.now(), Duration.ofHours(1));
        assertDoesNotThrow(() -> Validator.validateFilm(film));
    }

    @Test
    void shouldNotValidateNullDescriptionFilm() {
        Film film = new Film(0, "name", null, LocalDate.now(), Duration.ofHours(1));
        assertThrows(ValidationException.class, () -> Validator.validateFilm(film));
    }
    
    @Test
    void shouldNotValidateNullDurationFilm() {
        Film film = new Film(0, "name", "description", LocalDate.now(), null);
        assertThrows(ValidationException.class, () -> Validator.validateFilm(film));
    }

    @Test
    void shouldNotValidateNullLocalDate() {
        Film film = new Film(0, "name", "description", null, Duration.ofHours(1));
        assertThrows(ValidationException.class, () -> Validator.validateFilm(film));
    }

    @Test
    void shouldNotValidateTooOldFilm() {
        Film film = new Film(0, "name", "description", LocalDate.of(1800, 1, 1), Duration.ofHours(1));
        assertThrows(ValidationException.class, () -> Validator.validateFilm(film));
    }

    @Test
    void shouldNotValidateNotPositiveDurationFilm() {
        Film film = new Film(0, "name", "description", LocalDate.now(), Duration.ZERO);
        assertThrows(ValidationException.class, () -> Validator.validateFilm(film));
        film.setDuration(Duration.ofHours(-1));
        assertThrows(ValidationException.class, () -> Validator.validateFilm(film));
    }

    @Test
    void shouldValidateCorrectFilm() {
        Film film = new Film(0, "name", "description", LocalDate.now(), Duration.ofHours(1));
        assertDoesNotThrow(() -> Validator.validateFilm(film));
    }
    
    

    @Test
    void shouldNotValidateNullEmailUser() {
        User user = new User(0, null, "login", "name", LocalDate.now());
        assertThrows(ValidationException.class, () -> Validator.validateUser(user));
    }

    @Test
    void shouldNotValidateNullLoginUser() {
        User user = new User(0, "email@gmail.com", null, "name", LocalDate.now());
        assertThrows(ValidationException.class, () -> Validator.validateUser(user));
    }

    @Test
    void shouldNotValidateNullBirthdayUser() {
        User user = new User(0, "email@gmail.com", "login", "name", null);
        assertThrows(ValidationException.class, () -> Validator.validateUser(user));
    }

    @Test
    void shouldNotValidateBlankEmailUser() {
        User user = new User(0, "", "login", "name", LocalDate.now());
        assertThrows(ValidationException.class, () -> Validator.validateUser(user));
    }

    @Test
    void shouldNotValidateIncorrectEmailUser() {
        User user = new User(0, "emailGmail.com", "login", "name", LocalDate.now());
        assertThrows(ValidationException.class, () -> Validator.validateUser(user));
    }

    @Test
    void shouldNotValidateBlankLoginUser() {
        User user = new User(0, "email@mail.com", "", "name", LocalDate.now());
        assertThrows(ValidationException.class, () -> Validator.validateUser(user));
    }

    @Test
    void shouldNotValidateIncorrectLoginUser() {
        User user = new User(0, "email@mail.com", "login login", "name", LocalDate.now());
        assertThrows(ValidationException.class, () -> Validator.validateUser(user));
    }

    @Test
    void shouldNotValidateFutureBirthdayUser() {
        User user = new User(0, "email@gmail.com", "login", "name", LocalDate.now().plusDays(1));
        assertThrows(ValidationException.class, () -> Validator.validateUser(user));
    }

    @Test
    void shouldChangeUserNameToLoginIfNameIsEmpty() {
        User user = new User(0, "email@gmail.com", "login", "", LocalDate.now());
        User fixedUser = new User(user);
        fixedUser.setName("login");
        assertEquals(fixedUser.toString(), Validator.validateUser(user).toString());
    }
}