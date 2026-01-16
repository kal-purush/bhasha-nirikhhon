package com.ssw.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/env")
public class EnvironmentsController {

    @Autowired
    private Environment environment;

    @GetMapping("/getDetails")
    public String showProperties() {
        return "Port Number: " + environment.getProperty("server.port") + "\n" +
                "Application Name: " + environment.getProperty("spring.application.name") + "\n" +
                "Database Url: " + environment.getProperty("spring.datasource.url");
    }

    @Value("${spring.datasource.driver-class-name}")
    private String dataSource;

    @GetMapping("/getDetailsViaValue")
    public String getDetailsViaValue() {
        return "Data Source Driver: " + dataSource;
    }
}