package com.example.springsecsection3;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScans;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
//@ComponentScans({ @ComponentScan("com.example.srpringsection3.controller"),
//		@ComponentScan("com.example.srpringsection3.config")})
//@EnableJpaRepositories("com.example.srpringsection3.repository")
//@EntityScan("com.example.srpringsection3.model")

public class Springsecsection3Application {

	public static void main(String[] args) {
		SpringApplication.run(Springsecsection3Application.class, args);
	}

}