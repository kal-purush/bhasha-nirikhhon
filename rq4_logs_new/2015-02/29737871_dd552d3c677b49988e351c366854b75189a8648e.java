package com.qa.controllers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.qa.paymentPlans.PaymentPlanService;
import com.qa.userAccounts.MongoUserService;

@Controller
public class SearchController {
	
	MongoUserService mongoUserService;
	PaymentPlanService paymentPlanService;
	
	@Autowired
    public SearchController( MongoUserService mongoUserService, PaymentPlanService paymentPlayService )
    {
		System.out.println("main_controller_init");
        this.mongoUserService = mongoUserService;
        this.paymentPlanService = paymentPlayService;
    }
	
	@RequestMapping(value="/Search.uspr")
	public String search(HttpServletRequest request, HttpServletResponse response) {
		
	}

}