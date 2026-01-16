package com.ashish.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class IndexController {

	@RequestMapping("/rest")
	public String index(){
		return "/WEB-INF/jsp/index.jsp";
	}
}