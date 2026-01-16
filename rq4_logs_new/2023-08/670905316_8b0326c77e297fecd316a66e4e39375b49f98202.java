package org.matgyeojo.controller;

import org.matgyeojo.dto.Users;
import org.matgyeojo.service.LoginService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@RestController
@RequestMapping("/login")
public class LoginController {

	private final LoginService loginService;
	
	//로그인
 	@PostMapping()
	public Users login(@RequestBody Users user) { 
  		return loginService.getUserById(user);
	}
}