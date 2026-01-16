package com.spstes.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

import com.spstes.model.UserInfo;
import com.spstes.service.CatService;
import com.spstes.service.LoginService;
import com.spstes.service.RegisterService;

@Controller
public class Hello {

	@Autowired
	CatService catService;
	@Autowired
	LoginService loginService;
	@Autowired
	RegisterService registerService;

	@RequestMapping("/")
	public ModelAndView index() {
		ModelAndView mv = new ModelAndView();

		String login_name = "monkey_king";
		String login_pwd = "monkey_king";
		String IDCard = "511304885801586121";
		UserInfo user = loginService.findUserByAccountAndPwd(login_name, login_pwd);
		System.out.println("findUserByAccountAndPwd() : " + user);
		UserInfo user2 = registerService.findUserByAccount(login_name);
		System.out.println("findUserByAccount() : " + user2);
		UserInfo user3 = registerService.finUserByIDCard(IDCard);
		System.out.println("finUserByIDCard() : " + user3);

		mv.setViewName("index");
		return mv;
	}
}