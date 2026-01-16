package com.zcc.login.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import com.zcc.login.service.UserService;
import com.zcc.login.vo.ChangePasswordRequest;

/**
 * Created by ZhangChicheng on 2017/11/17.
 */
@Controller
public class UserController {
	@Autowired
	private UserService userService;
	@PostMapping("/changePassword")
	public ResponseEntity<Boolean> changePassword(@RequestBody @Valid ChangePasswordRequest request){
		return new ResponseEntity(userService.changePassword(request),HttpStatus.OK);
	}
}