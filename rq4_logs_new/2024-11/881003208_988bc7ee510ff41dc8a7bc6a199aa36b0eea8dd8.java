package ru.yandex.practicum.filmorate.controller;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.yandex.practicum.filmorate.model.User;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class UserControllerTest {

    private UserController userController;
    User user;

    @BeforeEach
    void setUp() {
        userController = new UserController();
        user = new User(0, "email@gmail.com", "login", "name", LocalDate.now());
    }

    @Test
    void shouldAddUser() {
        userController.createUser(user);
        assertEquals(userController.getUsers().toString(), List.of(user).toString());
    }

    @Test
    void shouldGenerateCorrectId() {
        userController.createUser(user);
        userController.createUser(new User(user));
        userController.createUser(new User(user));

        List<Long> ids = userController.getUsers().stream().map(User::getId).toList();
        assertEquals(ids.get(0), ids.get(1) - 1);
        assertEquals(ids.get(1), ids.get(2) - 1);
    }

    @Test
    void shouldUpdateUser() {
        userController.createUser(user);
        User updatedUser = new User(user);
        updatedUser.setName("updatedUser");

        userController.updateUser(updatedUser);
        assertEquals(userController.getUsers().toString(), List.of(updatedUser).toString());
    }

    @Test
    void shouldReturnEmptyListWhenNoUsersFound() {
        assertEquals(0, userController.getUsers().size());
    }
}