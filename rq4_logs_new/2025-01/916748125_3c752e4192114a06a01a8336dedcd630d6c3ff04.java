package ru.yandex.practicum.filmorate.controller;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.filmorate.exception.ValidationException;
import ru.yandex.practicum.filmorate.model.User;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/users")
public class UserController {

    private final Map<Long, User> users = new HashMap<>();

    private int nextId = 0;

    @GetMapping
    public Collection<User> getAllUsers() {
        return users.values();
    }

    @PostMapping
    public User addUser(@Valid @RequestBody User user) {
        if (user.getName() == null || user.getName().isBlank()) {
            log.warn("У пользователя отсутствует имя, вместо имени будет использоваться логин: " + user.getLogin());
            user.setName(user.getLogin());
        }
        if (user.getBirthday() == null || user.getBirthday().atStartOfDay(ZoneId.of("UTC")).toInstant().isAfter(Instant.now())) {
            String mes = "Дата рождения не может быть в будущем";
            log.error(mes);
            throw new ValidationException(mes);
        }
        user.setId(getNextId());
        users.put(user.getId(), user);
        log.info("Юзер с id: " + user.getId() + " успешно добавлен");
        return user;
    }

    @PutMapping
    public User updateUser(@Valid @RequestBody User newUser) {
        users.put(newUser.getId(), newUser);
        log.info("Юзер с id: " + newUser.getId() + " успешно изменён");
        return newUser;
    }

    private long getNextId() {
        return ++nextId;
    }
    
}