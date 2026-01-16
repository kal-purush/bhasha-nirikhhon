package com.example.userservice.controller;

import com.example.userservice.vo.Greeting;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RequestMapping("/")
@RequiredArgsConstructor
@RestController
public class UserController {

    private final Greeting greeting;

    @GetMapping("/health-check")
    public String healthCheck() {
        return "User Service is up and running!";
    }

    @GetMapping("/welcome")
    public String welcome() {
        return greeting.getMessage();
    }
}