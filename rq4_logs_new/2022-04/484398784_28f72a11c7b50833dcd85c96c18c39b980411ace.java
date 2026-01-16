package com.atamie.todo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
@ComponentScan({"com.atamie.todo"})
public class TodoInMemoryApplication {

	public static void main(String[] args)
	{
		SpringApplication.run(TodoInMemoryApplication.class, args);
	}

}