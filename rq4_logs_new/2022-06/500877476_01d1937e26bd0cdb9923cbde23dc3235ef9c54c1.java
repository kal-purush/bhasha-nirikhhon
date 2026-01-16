package com.infinidium.postgresql.admin.products;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;

@SpringBootApplication(exclude = SecurityAutoConfiguration.class)
public class SpringBootMicroserviceProductApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBootMicroserviceProductApplication.class, args);
	}

}