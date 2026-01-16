package com.iqmsoft.jsf;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import com.iqmsoft.jsf.scope.SessionResolver;


@SpringBootApplication
@ComponentScan(scopeResolver = SessionResolver.class)
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}