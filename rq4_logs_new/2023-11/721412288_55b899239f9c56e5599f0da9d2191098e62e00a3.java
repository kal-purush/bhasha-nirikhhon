package com.smyr.showmeyourrecipe.controller;

import com.smyr.showmeyourrecipe.dto.UserResponseDto;
import com.smyr.showmeyourrecipe.entity.User;
import com.smyr.showmeyourrecipe.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping( "/api" )
public class UserController {
	private final UserService userService;

	@Autowired
	public UserController( UserService userService ) {
		this.userService = userService;
	}

	@GetMapping( "/user/{userId}" )
	public ResponseEntity< UserResponseDto > getUser( @PathVariable long userId ) {
		var userResponseDto = this.userService.getUser( userId );

		return ResponseEntity.ok( userResponseDto );
	}
}