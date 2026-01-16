package ru.otus.hw;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
		System.out.println("http://localhost:9777/book - список книг");
		System.out.println("http://localhost:9777/book/new - создание книги");
		System.out.println("http://localhost:9777/book/edit_book?id=1 - редактирование книги");
		System.out.println();
		System.out.println("http://localhost:9777/authors - список авторов");
		System.out.println("http://localhost:9777/genres - список жанров");
	}

}