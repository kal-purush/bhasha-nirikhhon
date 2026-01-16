package com.ilicanspecialeducation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class IlicanSpecialEducationMsApplication {

    public static void main(String[] args) {
        SpringApplication.run(IlicanSpecialEducationMsApplication.class, args);
    }

}