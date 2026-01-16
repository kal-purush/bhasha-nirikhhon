package org.matgyeojo.controller;

import org.matgyeojo.dto.Users;
import org.matgyeojo.service.LoginService;
import org.matgyeojo.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@RestController
@RequestMapping("/card")
public class UserController {
	
	@Autowired
	private UserService userService;
	
	//카드 등록
	@PutMapping(value = "/register")
	public Users registerCard(@RequestBody Users user){
		return userService.registerCard(user);
	}
}