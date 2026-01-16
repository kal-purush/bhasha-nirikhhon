package ru.yandex.practicum.filmorate.controller;

import jakarta.validation.constraints.Positive;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.filmorate.model.User;
import ru.yandex.practicum.filmorate.service.UserService;

import java.util.*;

@RequiredArgsConstructor
@Slf4j
@Validated
@RestController
@RequestMapping("/users/{id}/friends")
public class FriendsController {

    private final UserService userService;

    @GetMapping
    public Collection<User> allFriends(@PathVariable("id")
                                       @Positive(message = "id должен быть положительным") Long userId) {
        return userService.allFriends(userId);
    }

    @GetMapping("/common/{otherId}")
    public Collection<User> commonFriends(@PathVariable("id")
                                          @Positive(message = "id должен быть положительным") Long userId,
                                          @PathVariable("otherId")
                                          @Positive(message = "otherId должен быть положительным") Long otherId) {
        return userService.commonFriends(userId, otherId);
    }

    @PutMapping("/{friendId}")
    public User addFriend(@PathVariable("id") @Positive(message = "id должен быть положительным") Long userId,
                          @PathVariable("friendId")
                          @Positive(message = "friendId должен быть положительным") Long friendId) {
        return userService.addFriend(userId, friendId);
    }

    @DeleteMapping("/{friendId}")
    public User removeFriend(@PathVariable("id") @Positive(message = "id должен быть положительным") Long userId,
                             @PathVariable("friendId")
                             @Positive(message = "friendId должен быть положительным") Long friendId) {
        return userService.removeFriend(userId, friendId);
    }
}