package com.uttam.project.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.uttam.project.model.User;
import com.uttam.project.service.UserService;

import lombok.Setter;

@RestController
@RequestMapping("/user")
class UserController {
	
	@Autowired
	@Setter
	UserService userServiceImpl;
	
	@GetMapping("/{id}")
	public User getUser(@PathVariable("id") Integer userId) {
		return userServiceImpl.getUser(userId);
	}
	
	@GetMapping("")
	public List<User>getAllUsers() {
		return userServiceImpl.getAllUsers();
	}

	@PostMapping("")
	public User createUser(@RequestBody User user) {
		return userServiceImpl.createUser(user);
	}
	
	@PutMapping("")
	public User updateUser(@RequestBody User user) {
		return userServiceImpl.updateUser(user);
	}
	
	@DeleteMapping("/{id}")
	public String deleteUser(@PathVariable("id") Integer userId) {
		return userServiceImpl.deleteUser(userId);
	}
	
	@GetMapping(params = {"lastName"})
	public List<User> getUsersByLastName(@RequestParam String lastName) {
		 return userServiceImpl.getUsersByLastName(lastName);
	}
	
	@GetMapping(params ="firstName")
	public List<User> getUsersByFirstName(@RequestParam String firstName) {
		 return userServiceImpl.getUsersByFirstName(firstName);
	}
	
}