package com.jamlee.ipadtracker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class IpadTrackerApplication {

    public static void main(String[] args) {
        SpringApplication.run(IpadTrackerApplication.class, args);
    }

}