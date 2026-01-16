package com.project_merge.jigu_travel.auth.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/auth")
public class AuthController {

    @GetMapping("/login")
    public String login() {
        return "login"; // login.html
    }

    @GetMapping("/register")
    public String register() {
        return "register"; // register.html
    }

    @GetMapping("/logout")
    public String logout() {
        return "logout"; // logout.html
    }
}