package ru.practicum.shareit;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@PropertySource(value = "application.properties")
public class ShareItServer {
    public static void main(String[] args) {
        SpringApplication.run(ShareItServer.class, args);
    }
}