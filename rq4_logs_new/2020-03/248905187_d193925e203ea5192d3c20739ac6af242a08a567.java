package com.hour.task.controller;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ComponentScan(basePackages = "hour")
@RequestMapping("/admin")
public class AdminController {

    @RequestMapping("/hello")
    String hello(String param){
        return param;
    }

}