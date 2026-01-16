package com.example.loginpassword;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;



@SpringBootApplication
@OpenAPIDefinition
public class LoginPasswordApplication {

    public static void main(String[] args) {
        SpringApplication.run(LoginPasswordApplication.class, args);
    }

}
