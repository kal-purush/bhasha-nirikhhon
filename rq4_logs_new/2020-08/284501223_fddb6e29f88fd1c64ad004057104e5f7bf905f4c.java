package com.react.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.react.demo.model.User;
import com.react.demo.repository.UserRepository;

@SpringBootApplication
public class ReactAppDemoApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(ReactAppDemoApplication.class, args);
	}

	@Autowired
	private UserRepository repo;
	@Override
	public void run(String... args) throws Exception {
		this.repo.save(new User(101,"ramesh","pachare","rampach@gmail.com"));
		this.repo.save(new User(101,"rajesh","pachare","rajpach@gmail.com"));
		this.repo.save(new User(101,"mahesh","pachare","mahpach@gmail.com"));
		
	}

}