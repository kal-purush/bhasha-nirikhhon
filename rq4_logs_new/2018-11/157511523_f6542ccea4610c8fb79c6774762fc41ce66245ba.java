package com.tuhj.controllers;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FirstController {

    @RequestMapping(value = "/hello")
    public String hello() {
        return "HelloWord SpringBoot 555";
    }
}

