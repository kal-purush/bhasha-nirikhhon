package com.construo.ff4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.autoconfigure.thymeleaf.ThymeleafAutoConfiguration;

@SpringBootApplication(exclude = {ThymeleafAutoConfiguration.class, SecurityAutoConfiguration.class})
public class FF4jApplication {

    public static void main(String[] args) {
        SpringApplication.run(FF4jApplication.class, args);
    }

    /*@Value("${somePropertyValue}")
    private boolean enableFeature;

    public void calculation() {
        boolean useNewAlgorithm = false;
        // useNewAlgorithm = true; // UNCOMMENT IF YOU ARE WORKING ON NEW CALCULATION
        if (useNewAlgorithm) {
            enhancedCalculation();
        } else {
            oldFashionedCalculation();
        }
    }

    private void oldFashionedCalculation() {
        // current implementation lives here
    }

    private void enhancedCalculation() {
        // TODO: implement better SR algorithm
    }*/
}