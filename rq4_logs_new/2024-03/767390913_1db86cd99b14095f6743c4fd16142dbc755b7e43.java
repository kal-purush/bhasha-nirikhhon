package ru.samgups.cakestudio;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.thymeleaf.extras.java8time.dialect.Java8TimeDialect;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

@SpringBootApplication
public class CakestudioApplication {

	public static void main(String[] args) {
		SpringApplication.run(CakestudioApplication.class, args);
	}

}