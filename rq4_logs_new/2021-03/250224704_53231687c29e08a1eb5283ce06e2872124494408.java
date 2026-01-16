package com.kk.springmvc;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class Addition {

	@RequestMapping("/")
	public String display() {
		return "index";
	}
}