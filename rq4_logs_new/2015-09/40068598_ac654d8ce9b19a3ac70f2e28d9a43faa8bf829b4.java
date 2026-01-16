package com.ksb.web.openapi.mobile.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ksb.openapi.entity.ResultEntity;

@Controller
@RequestMapping("/mobile/pay")
public class AppZFController {

	@RequestMapping("create_charge")
	public @ResponseBody
	       ResultEntity createCharge(){
		
		
		return null;
	}
	
	
	
}