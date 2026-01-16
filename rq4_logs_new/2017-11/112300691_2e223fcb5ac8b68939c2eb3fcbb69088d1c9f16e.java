package com.example.demo;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
public class DemoApplication {
    @RestController
    public static class RootController {
        @GetMapping("/")
        public String root() {
            return "Hello";
        }

        @GetMapping("/l")
        public String longMessage(@RequestParam(value = "n", defaultValue = "1024") int n) {
            return Stream.generate(() -> ".").limit(n).collect(Collectors.joining(""));
        }

    }

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}