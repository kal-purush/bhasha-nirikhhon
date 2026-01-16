package com.example.movie_client;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
public class MovieClientApplication {

	public static void main(String[] args) {
		SpringApplication.run(MovieClientApplication.class, args);
	}

}