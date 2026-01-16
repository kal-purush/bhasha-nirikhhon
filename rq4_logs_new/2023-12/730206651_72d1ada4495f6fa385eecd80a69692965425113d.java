package com.team2.lb.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("payment")
public class PaymentController {
	
	@GetMapping("toss")
	public String toss() {
		return "payment/toss";
	}
	
	@GetMapping("success")
	public String success() {
		return "payment/success";
	}
}