package ru.yandex.practicum.filmorate.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.filmorate.model.User;

import javax.validation.Valid;
import ru.yandex.practicum.filmorate.exception.ValidationException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/users")
@Slf4j
public class UserController {

    private final Map<Integer, User> users = new HashMap<>();

    private static int userIdGen = 0;

    @PostMapping
    public User create(@Valid @RequestBody User user) {

        log.info("POST-запрос на эндпоинт /users");

        if (users.containsKey(user.getId())) {
            throw new ValidationException("Пользователь с таким id уже создан");
        }

        if (isValidUser(user)) {
            if(user.getName().isEmpty()) {
                user.setName(user.getLogin());
            }

            int currId = ++userIdGen;
            user.setId(currId);
            users.put(currId, user);

            log.info("Новый пользователь с логином: " + "'" + user.getLogin() + "'" + " создан");

        }

        return user;
    }

    @PutMapping
    public User update(@Valid @RequestBody User user) {

        log.info("PUT-запрос на обновление пользователя с id={}", user.getId());

        if (!users.containsKey(user.getId())) {
            create(user);
        }

        if (isValidUser(user)) {
            if(user.getName().isEmpty()) {
                user.setName(user.getLogin());
            }
            users.put(user.getId(), user);
            log.info("Пользователь с id={} обновлен", user.getId());
        }

        return user;
    }

    @GetMapping
    public List<User> getAll() {

        return new ArrayList<>(users.values());
    }


    private boolean isValidUser(User user) {
        if (user.getEmail().isEmpty()) {
            throw new ValidationException("Email не должен быть пустым");
        }
        if (user.getLogin().isEmpty() || user.getLogin().contains(" ")) {
            throw new ValidationException("Логин не должен быть пустым/содержать пробелы");
        }
        if (user.getBirthday().isAfter(LocalDate.now())) {
            throw new ValidationException("Дата рождения не должна быть в будущем");
        }
        return true;
    }
}