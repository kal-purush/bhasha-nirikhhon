package com.kanban.spring;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class KanbanBoardProjectApplication {

	// index page Endung von url
	@GetMapping("/start")
	public String sayHello() {
		return "Hello World!";
	}

	public static void main(String[] args) {
		SpringApplication.run(KanbanBoardProjectApplication.class, args);
	}

}