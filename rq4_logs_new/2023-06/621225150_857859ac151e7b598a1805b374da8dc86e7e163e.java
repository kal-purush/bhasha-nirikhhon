package ru.otus.spring.homework.oke;

import com.github.cloudyrock.spring.v5.EnableMongock;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@EnableMongoRepositories
@EnableMongock
@SpringBootApplication
public class SpringHw08Application {
    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(SpringHw08Application.class);
    }
}