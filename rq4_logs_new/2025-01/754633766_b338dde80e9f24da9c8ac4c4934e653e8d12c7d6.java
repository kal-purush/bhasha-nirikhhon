package com.kraft.atend.controller;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.kraft.atend.annotations.JwtFilter;
import com.kraft.atend.model.UserResponseDto;
import com.kraft.atend.service.UserService;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/user")
@RequiredArgsConstructor
public class UserController {
	
	private final UserService userService;
	
	@PostMapping("/get-user-list")
	@JwtFilter
	public ResponseEntity<List<UserResponseDto>> getUserList(){
		return ResponseEntity.ok(userService.getUserList());
	}
	
	@PostMapping("/find-by-id")
	@JwtFilter
	public ResponseEntity<UserResponseDto> findUserById(@RequestParam long id){
		return ResponseEntity.ok(userService.findUserById(id));
	}

}