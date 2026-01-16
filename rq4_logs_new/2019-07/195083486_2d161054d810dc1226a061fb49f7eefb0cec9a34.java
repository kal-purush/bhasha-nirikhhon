package com.fse.userapi.web.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

public class WelcomeController {

    @RestController
    public class HomeController {

        @GetMapping({"/welcome"})
        public String homePage() {
            return "/home";
        }
    }
}