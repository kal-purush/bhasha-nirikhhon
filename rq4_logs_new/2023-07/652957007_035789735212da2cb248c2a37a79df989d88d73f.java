package ru.practicum.ewmmainservice.user.controller.privateController;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import ru.practicum.ewmmainservice.user.dto.UserIdDto;
import ru.practicum.ewmmainservice.user.model.User;
import ru.practicum.ewmmainservice.user.service.UserService;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;

@Slf4j
@RestController
@RequestMapping(path = "/users/{userId}/following")
public class UserPrivateController {

    private final UserService userService;

    @Autowired
    public UserPrivateController(UserService userService) {
        this.userService = userService;
    }

    @ResponseStatus(HttpStatus.CREATED)
    @PostMapping
    public User createFolowing(@PathVariable int userId,
                               @Valid @RequestBody UserIdDto following,
                               HttpServletRequest request) {
        log.info("Получен запрос к эндпоинту: '{} {}', Строка параметров запроса: '{}'",
                request.getMethod(), request.getRequestURI(), request.getQueryString());
        return userService.createFolowing(userId, following);
    }

    @ResponseStatus(HttpStatus.NO_CONTENT)
    @DeleteMapping("/{folowingId}")
    public void deleteUser(@PathVariable int userId,
                           @PathVariable int folowingId,
                           HttpServletRequest request) {
        log.info("Получен запрос к эндпоинту: '{} {}', Строка параметров запроса: '{}'",
                request.getMethod(), request.getRequestURI(), request.getQueryString());
        userService.removeFolowing(userId, folowingId);
    }

}