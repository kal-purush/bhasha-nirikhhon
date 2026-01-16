package com.revature.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.revature.json.AccountJSON;
import com.revature.service.AccountService;
import com.revature.util.BeanToJSONUtil;

@CrossOrigin
@RestController
@RequestMapping("/account")
public class AccountController {

	@Autowired
	private AccountService accountService;
	@Autowired
	private BeanToJSONUtil btju;

	@GetMapping("/{id}")
	public ResponseEntity<AccountJSON> getAccount(@PathVariable int id) {
		return new ResponseEntity<AccountJSON>(btju.accountToJSON(accountService.getAccount(id)), HttpStatus.OK);
	}

	@GetMapping("/email/{email}/password/{password}")
	public ResponseEntity<AccountJSON> login(@PathVariable String email, @PathVariable String password) {
		return new ResponseEntity<AccountJSON>(btju.accountToJSON(accountService.login(email, password)), HttpStatus.OK);
	}
	
}