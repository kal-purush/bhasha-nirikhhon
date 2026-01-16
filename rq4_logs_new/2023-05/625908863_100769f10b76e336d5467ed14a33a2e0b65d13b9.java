package com.brainstrom.meokjang.controller;

import com.brainstrom.meokjang.domain.User;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;

@Controller
public class AuthController {

    @PostMapping("/users/create")
    public String create(UserForm userForm) {
        User user = new User(userForm);

        return "redirect:/";
    }

//    @PostMapping("/users/login")
//    public String login() {
//        return "redirect:/";
//    }
}