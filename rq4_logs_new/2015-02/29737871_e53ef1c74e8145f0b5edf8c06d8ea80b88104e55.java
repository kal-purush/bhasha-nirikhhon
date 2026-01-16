package com.qa.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.qa.security.MongoUserService;

@Controller
public class UserController {
	
	MongoUserService mongoUserService;
	
	@Autowired
    public UserController( MongoUserService mongoUserService )
    {
		System.out.println("controller_init");
        this.mongoUserService = mongoUserService;
    }
	
	@RequestMapping(value="/SearchPage.spr", method=RequestMethod.GET)
	public String login() {
		
		return "SearchPage";
		
	}
	
	@RequestMapping(value="/registerdetails.spr")
	public String newUser(@RequestParam String username, @RequestParam String password) {
		System.out.println("Hello");
		
		mongoUserService.addNewUser(username, password);
		
		return "about";
		
	}

}