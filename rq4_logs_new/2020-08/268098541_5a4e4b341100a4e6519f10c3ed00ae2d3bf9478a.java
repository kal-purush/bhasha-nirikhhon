package ru.otus.spring.rest_book_info_app.controller;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@SpringBootApplication
@ComponentScan(basePackages = {
    "ru.otus.spring.rest_book_info_app",
    "ru.otus.spring.web_ui_book_info_app.repository",
    "ru.otus.spring.web_ui_book_info_app.service"
})
@EnableMongoRepositories("ru.otus.spring.web_ui_book_info_app.repository")
public class Main {
    public static void main(String[] args) {
        System.out.println("HERE I AM");
        SpringApplication.run(Main.class);
    }
}