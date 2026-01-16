package ru.yandex.practicum.filmorate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.filmorate.controller.UserController;
import ru.yandex.practicum.filmorate.exception.ValidationException;
import ru.yandex.practicum.filmorate.model.User;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class UserControllerTest {

    private UserController userController;
    private User user;

    @BeforeEach
    public void setUp() {
        userController = new UserController();

        user = User.builder()
                .email("some_dude@yandex.ru")
                .login("dude01")
                .name("Mikey")
                .birthday(LocalDate.of(1991, 6, 20))
                .build();
    }

    @Test
    public void shouldCreateUserAllFieldsAreCorrect() {

        User mikey = userController.create(user);

        assertEquals(mikey, user);
        assertEquals(1, userController.getAll().size(), "Размер списка пользователей равен 1");
    }


    // ошибка валидации если email пустой
    @Test
    public void shouldNotCreateUserIfEmailIsEmpty() {

        user.setEmail("");
        assertThrows(ValidationException.class, () -> userController.create(user));
        assertEquals(0, userController.getAll().size(), "Список пользователей должен быть пустым");
    }

    // ошибка валидации если login пустой
    @Test
    public void shouldNotCreateUserIfLoginIsEmpty() {

        user.setLogin("");
        assertThrows(ValidationException.class, () -> userController.create(user));
        assertEquals(0, userController.getAll().size(), "Список пользователей должен быть пустым");
    }

    // ошибка валидации если login содержит пробелы
    @Test
    public void shouldNoAddUserWhenUserLoginIsContainsSpaces() {
        user.setLogin("dude 01");
        assertThrows(ValidationException.class, () -> userController.create(user));
        assertEquals(0, userController.getAll().size(), "Список пользователей должен быть пустым");
    }

    // при пустом имени должен отображаться логин
    @Test
    public void shouldCreateUserWithLoginValueInNameFieldIfNameIsEmpty() {

        user.setName("");

        User dude01 = userController.create(user);
        assertEquals(dude01, user);
        assertEquals("dude01", dude01.getName());
        assertEquals(1, userController.getAll().size(), "Размер списка пользователей равен 1");
    }

    // ошибка валидации если birthday в будущем
    @Test
    public void shouldNotCreateUserIfBirthdayIsAboveNow() {

        user.setBirthday(LocalDate.of(2991, 1, 25));
        assertThrows(ValidationException.class, () -> userController.create(user));
        assertEquals(0, userController.getAll().size(), "Список пользователей должен быть пустым");
    }

}