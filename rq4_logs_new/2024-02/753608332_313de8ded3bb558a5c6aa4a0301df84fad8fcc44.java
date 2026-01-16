package com.example.gameCafe;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan(basePackages = "com.example.gameCafe")
@EnableJpaRepositories(basePackages = "com.example.gameCafe.repositories")
public class GameCafeApplication {

	public static void main(String[] args) {
		SpringApplication.run(GameCafeApplication.class, args);
	}

}