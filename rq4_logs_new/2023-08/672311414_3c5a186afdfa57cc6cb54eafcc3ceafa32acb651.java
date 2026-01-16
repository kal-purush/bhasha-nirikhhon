package ru.scarlet.authservice.controller;

import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.scarlet.authservice.dto.SignInRequest;
import ru.scarlet.authservice.dto.SignUpRequest;
import ru.scarlet.authservice.entity.User;
import ru.scarlet.authservice.exceptions.UserAlreadyExistsException;
import ru.scarlet.authservice.service.UserService;

@RestController
@RequestMapping("/api/v1/users")
@RequiredArgsConstructor
public class UserController {
    private final UserService userService;

    @PostMapping("/signin")
    public ResponseEntity<User> signin(@RequestBody SignInRequest request, HttpServletRequest httpServletRequest) {
        User user = userService.getUserByUsername(request.getUsername());
        return ResponseEntity.ok(user);
    }

    @PostMapping("/signup")
    public ResponseEntity<?> signUp(@RequestBody SignUpRequest signUpRequest, HttpServletRequest httpServletRequest) {
        if (!userService.userExists(signUpRequest.getUsername())) {
            User user = userService.createUser(signUpRequest);
            return new ResponseEntity<>(user, HttpStatusCode.valueOf(201));
        } else throw new UserAlreadyExistsException();
    }
}