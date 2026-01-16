package com.ssw.config;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;



@Component
public class AppPropertiesLogger {

    @Autowired
    private Environment environment;

    @PostConstruct
    public void logProperties() {
        System.out.println("Application Properties:");
        System.out.println("Application Name: " + environment.getProperty("spring.application.name"));
        System.out.println("Server Port: " + environment.getProperty("server.port"));
        System.out.println("Database URL: " + environment.getProperty("spring.datasource.url"));
        System.out.println("Database Driver: " + environment.getProperty("spring.datasource.driver-class-name"));
        System.out.println("Database Username: " + environment.getProperty("spring.datasource.username"));
        System.out.println("Database Password: " + environment.getProperty("spring.datasource.password"));
        System.out.println("Hibernate Dialect: " + environment.getProperty("spring.jpa.properties.hibernate.dialect"));
        System.out.println("Hibernate DDL Auto: " + environment.getProperty("spring.jpa.hibernate.ddl-auto"));
        System.out.println("Show SQL: " + environment.getProperty("spring.jpa.show-sql"));
    }
    }

