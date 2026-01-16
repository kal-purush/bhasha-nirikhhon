package com.evoke.tms;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan("com.evoke.tms")
public class EmployeeTmsApplication {

    public static void main(String[] args) {
        SpringApplication.run(EmployeeTmsApplication.class, args);
    }
}