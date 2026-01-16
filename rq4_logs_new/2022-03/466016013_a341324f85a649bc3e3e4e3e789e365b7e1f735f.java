package com.example.testRedis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TestRedisApplication {

	private static final Logger logger = LoggerFactory.getLogger(TestRedisApplication.class);

	public static void main(String[] args) {

		SpringApplication.run(TestRedisApplication.class, args);
		logger.info("Springboot redis application started successfully");
	}

}