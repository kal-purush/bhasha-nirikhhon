package com.microservice.hruser.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.microservice.hruser.entities.User;
import com.microservice.hruser.repositories.UserRepository;

@RestController
@RequestMapping(value = "hr-user")
public class UserController {
	
	@Autowired
	private UserRepository repository;
	
	@GetMapping(value = "/users/{id}")
	public ResponseEntity<User> findById(@PathVariable Long id){
		User obj = repository.findById(id).get();
		return ResponseEntity.ok().body(obj);
	}
	
	@GetMapping(value = "/users/search")
	public ResponseEntity<User> findByEmail(@RequestParam String email){
		User obj = repository.findByEmail(email);
		return ResponseEntity.ok().body(obj);
	}

}