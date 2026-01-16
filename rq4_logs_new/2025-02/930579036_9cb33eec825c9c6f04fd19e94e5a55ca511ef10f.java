package ru.yandex.practicum.filmorate.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.filmorate.exception.ValidationException;
import ru.yandex.practicum.filmorate.model.User;

import java.time.LocalDate;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/users")
public class UserController {

    private final Map<Long, User> users = new HashMap<>();
    private static final String EMPTY_EMAIL_ERR = "Email не может быть пустой и должен содержать символ @";
    private static final String EMPTY_LOGIN_ERR = "Логин не может быть пустым и содержать пробелы";
    private static final String BIRTHDAY_ERR = "Дата рождения не может быть в будущем";
    private static final String DUPL_EMAIL_ERR = "Этот имейл уже используется";
    private static final String ID_ERR = "Id должен быть указан";

    @GetMapping
    public Collection<User> findAll() {
        log.trace("Начинаем возвращать всех пользователей");
        return users.values();
    }

    @PostMapping
    public User create(@RequestBody User user) {
        // проверяем выполнение необходимых условий
        log.trace("Начинаем создавать пользователя");
        log.trace(user.toString());

        if (user.getEmail() == null || user.getEmail().isBlank() || !user.getEmail().contains("@")) {
            log.error(EMPTY_EMAIL_ERR);
            throw new ValidationException(EMPTY_EMAIL_ERR);
        }
        if (user.getLogin() == null || user.getLogin().isBlank() || user.getLogin().contains(" ")) {
            log.error(EMPTY_LOGIN_ERR);
            throw new ValidationException(EMPTY_LOGIN_ERR);
        }
        if (user.getBirthday().isAfter(LocalDate.now())) {
            log.error(BIRTHDAY_ERR);
            throw new ValidationException(BIRTHDAY_ERR);
        }
        if (users.values().stream()
                .map(User::getEmail)
                .anyMatch(userEmail -> userEmail.equals(user.getEmail()))
        ) {
            log.error(DUPL_EMAIL_ERR);
            throw new ValidationException(DUPL_EMAIL_ERR);
        }
        log.trace("Проверки пройдены");

        // формируем дополнительные данные
        User usr = User.builder()
            .id(getNextId())
            .name((user.getName() == null || user.getName().isBlank()) ? user.getLogin() : user.getName())
            .email(user.getEmail())
            .login(user.getLogin())
            .birthday(user.getBirthday())
            .build();

        // сохраняем новую публикацию в памяти приложения
        users.put(usr.getId(), usr);

        log.trace("Успешно создали пользователя");
        log.trace(usr.toString());
        return usr;
    }

    @PutMapping
    public User update(@RequestBody User newUser) {
        log.trace("Начинаем обновлять пользователя");
        log.trace(newUser.toString());
        // проверяем необходимые условия
        if (newUser.getId() == null) {
            log.error(ID_ERR);
            throw new ValidationException(ID_ERR);
        }
        if (newUser.getEmail() == null || newUser.getEmail().isBlank() || !newUser.getEmail().contains("@")) {
            log.error(EMPTY_EMAIL_ERR);
            throw new ValidationException(EMPTY_EMAIL_ERR);
        }
        if (newUser.getLogin() == null || newUser.getLogin().isBlank() || newUser.getLogin().contains(" ")) {
            log.error(EMPTY_LOGIN_ERR);
            throw new ValidationException(EMPTY_LOGIN_ERR);
        }
        if (newUser.getBirthday().isAfter(LocalDate.now())) {
            log.error(BIRTHDAY_ERR);
            throw new ValidationException(BIRTHDAY_ERR);
        }
        if (users.containsKey(newUser.getId())) {
            User oldUser = users.get(newUser.getId());
            if (!newUser.getEmail().equals(oldUser.getEmail())) {
                if (users.values().stream()
                        .map(User::getEmail)
                        .anyMatch(userEmail -> userEmail.equals(newUser.getEmail()))
                ) {
                    log.error(DUPL_EMAIL_ERR);
                    throw new ValidationException(DUPL_EMAIL_ERR);
                }
            }
            log.trace("Проверки пройдены");

            oldUser.setEmail(newUser.getEmail());
            oldUser.setName((newUser.getName() == null || newUser.getName().isBlank())
                    ? newUser.getLogin() : newUser.getName());
            oldUser.setLogin(newUser.getLogin());
            oldUser.setBirthday(newUser.getBirthday());

            log.trace("Успешно обновили пользователя");
            log.trace(oldUser.toString());
            return oldUser;
        } else {
            log.error("Пользователь с id = {} не найден", newUser.getId());
            throw new ValidationException("Пользователь с id = " + newUser.getId() + " не найден");
        }
    }

    // вспомогательный метод для генерации идентификатора нового поста
    private long getNextId() {
        long currentMaxId = users.keySet()
                .stream()
                .mapToLong(id -> id)
                .max()
                .orElse(0);
        return ++currentMaxId;
    }
}