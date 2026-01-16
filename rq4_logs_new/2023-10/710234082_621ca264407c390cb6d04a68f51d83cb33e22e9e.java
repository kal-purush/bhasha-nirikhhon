package io.namoosori.rest.controller;

import java.util.List;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import io.namoosori.rest.entity.User;
import io.namoosori.rest.service.UserService;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@RestController
public class UserController {
	private final UserService userService;
	
	@PostMapping("/users")
	public String register(@RequestBody User newUser) {
		// Post 요청이기 때문에 requestBody로 데이터가 넘어온다.
		return userService.register(newUser);
	}
	
	@GetMapping("/users/{id}")
	public User find(@PathVariable String id) {
		return userService.find(id);
	}
	
	@GetMapping("/users")
	public List<User> findAll() {
		return this.userService.findAll();
	}
	
	@PutMapping("/users/")
	public void modify(@RequestBody User newUser) {
		this.userService.modify(newUser);
	}
	
	@DeleteMapping("/users/{id}")
	public void remove(@PathVariable String id) {
		this.userService.remove(id);
	}
}