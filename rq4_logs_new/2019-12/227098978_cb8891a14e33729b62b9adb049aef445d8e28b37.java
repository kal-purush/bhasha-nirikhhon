package com.zhangwei.cms.controller.admin;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/admin/")
public class AdminController {

	@RequestMapping("/")
	public String login() {
		return "admin/login";
	}
	@RequestMapping("/home")
	public String home() {
		return "admin/home";
	}
	@RequestMapping("/user")
	public String user() {
		return "admin/user";
	}
	@RequestMapping("/welcome")
	public String welcome() {
		return "admin/welcome";
	}
}