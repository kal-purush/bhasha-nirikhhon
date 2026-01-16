package com.example.movielist;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.example.movielist.mapper")
public class MovielistApplication {

    public static void main(String[] args) {
        SpringApplication.run(MovielistApplication.class, args);
    }
}