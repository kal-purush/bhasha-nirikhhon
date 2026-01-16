package com.example.mjhbs1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController

@SpringBootApplication
public class Taller01Application {
	
	@RequestMapping(value="/")
	public String index() {
		return "<h1> Holiwis <h1>";
	}

	public static void main(String[] args) {
		SpringApplication.run(Taller01Application.class, args);
	}

}