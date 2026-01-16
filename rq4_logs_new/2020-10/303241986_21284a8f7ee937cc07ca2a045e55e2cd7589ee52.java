package com.evaluation.service;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com")
public class MainService {
    public static void main(String[] args) {
        SpringApplication.run(MainService.class,args);
    }
}