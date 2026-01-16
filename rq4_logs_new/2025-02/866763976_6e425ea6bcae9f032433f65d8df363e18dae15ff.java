package ru.otus.hw;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

//http://localhost:8080
//http://localhost:8080/authors
//http://localhost:8080/genres
//http://localhost:8080/comments?id=1
//http://localhost:8080/edit?id=1
//http://localhost:8080/edit?id=111
@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

}