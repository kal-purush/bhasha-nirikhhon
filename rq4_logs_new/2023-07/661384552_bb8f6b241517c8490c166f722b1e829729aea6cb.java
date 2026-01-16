package com.springMVC.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.springMVC.entity.User;
import com.springMVC.service.UserService;

@Controller
public class HomeController {

	@Autowired
	private UserService userService; // IOC container know that i have to create object of this through @Service
										// annotation in ServiceImpl class

	@RequestMapping("/home")
	public String home() {
		return "home";
	}

	@RequestMapping("/register")
	public String signup() {
		return "register";
	}

	@RequestMapping(path = "/createUser", method = RequestMethod.POST)
	public String registerUser(@ModelAttribute User user, @RequestParam("img") String img, Model m) {
		user.setImage(img);
		//System.out.println(user);
		userService.saveUser(user);
		return "success"; // return = redirect to success page:
	}

}