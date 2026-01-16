package com.se.fit.TravelProject.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.se.fit.TravelProject.entities.User;
import com.se.fit.TravelProject.service.AccountService;
import com.se.fit.TravelProject.service.DepartureService;
import com.se.fit.TravelProject.service.DestinationService;
import com.se.fit.TravelProject.service.TravelPackageService;
import com.se.fit.TravelProject.service.UserService;

import jakarta.servlet.http.HttpSession;

@Controller
@RequestMapping("/Cart")
public class CartController {
	private TravelPackageService travelPackageService;
	private DepartureService departureService;
	private DestinationService destinationService;
	private AccountService accountService;
	private UserService userService;
	
	
	@Autowired
	public CartController(TravelPackageService travelPackageService, DepartureService departureService,
			DestinationService destinationService  ,AccountService accountService,UserService userService) {
		super();
		this.travelPackageService = travelPackageService;
		this.departureService = departureService;
		this.destinationService = destinationService;
		this.accountService = accountService;
		this.userService = userService;
	}
	
	@GetMapping("/showCart")
	public String showCart() {
		return "GioHang";
	}
}