package com.faisal.exercise.rezdy;

import static org.springframework.boot.SpringApplication.run;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication
@Configuration
public class RezdyApplicationLauncher {

    public static void main(String[] args) {
        run(RezdyApplicationLauncher.class, args);
    }
}