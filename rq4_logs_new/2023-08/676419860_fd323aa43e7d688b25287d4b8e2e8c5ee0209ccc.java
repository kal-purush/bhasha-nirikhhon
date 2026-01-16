package com.fse.shoppingapp;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.fse.shoppingapp.models.ERole;
import com.fse.shoppingapp.models.Role;
import com.fse.shoppingapp.repository.RoleRepository;


@SpringBootApplication
public class ShoppingappApplication implements CommandLineRunner {
	
	@Autowired
	private RoleRepository roleRepository;
	
	
	@Autowired
	private MongoTemplate mongoTemplate;

	public static void main(String[] args) {
		SpringApplication.run(ShoppingappApplication.class, args);
	}

	public void run(String... args) throws Exception {
		
		mongoTemplate.dropCollection("roles");
		mongoTemplate.dropCollection("users");
		
		Role admin = new Role(ERole.ROLE_ADMIN);
		Role user = new Role(ERole.ROLE_USER);

		roleRepository.saveAll(List.of(admin,user));
		
		
		
	}

}