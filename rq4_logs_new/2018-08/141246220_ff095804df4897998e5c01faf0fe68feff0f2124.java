package com.spstes.controller;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@Scope("prototype")
@RequestMapping("/admin")
public class Admin {

	@RequestMapping("/kaocijihua")
	public void kaociManagement() {

	}

	@RequestMapping("/biyetiaojian")
	public void graguateCondition() {

	}

	@RequestMapping("/kecheng")
	public void course() {

	}

	@RequestMapping("/kaodian")
	public void examPoint() {

	}

	@RequestMapping("/kaodiankaikaozhuanye")
	public void examStarting() {

	}

	@RequestMapping("/kechengshoufeibiaozhun")
	public void examCharge() {

	}

	@RequestMapping("/zhuanyeguanli")
	public void ProfessionManagement() {

	}

	@RequestMapping("/kaoshengchengji")
	public void studentMarks() {

	}

	@RequestMapping("/zhuanyekecheng")
	public void ProfessionCourse() {

	}
}