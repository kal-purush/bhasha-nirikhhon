package com.example.hellospring.controllers;

//02/07/24

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

//	타입 단독 매핑
//	클래스가 상위 url 처리, 하위 url -> 메서드 이름과 매핑
@RequestMapping("/guestbook/*")
@Controller
public class GuestbookController {
	@ResponseBody
	@RequestMapping		//	하위 url 패턴을 명시하지 않음 
	public String list() {	//	-> /guestbook/list 에 연결
		return "<h1>Guestbook List</h1>";
	}
	
	@ResponseBody
	@RequestMapping
	public String add() {
		return "<h1>Guestbook Add</h1>";
	}
	
	@ResponseBody
	@RequestMapping
	public String delete() {
		return "<h1>Guestbook Delete</h1>";
	}
	

}