package com.demo.atomic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

@RestController
@SpringBootApplication
public class AtomicApplication {

	private static final Logger LOGGER = LoggerFactory.getLogger(AtomicApplication.class);
    public static void main(String[] args) {
        SpringApplication.run(AtomicApplication.class, args);
    }
    
    
    @RequestMapping("/sayHello")
    public String sayHello() {
    	LOGGER.info("returning Hello");
    	return "Hello:";
    }

}