package com.castillo.omikujiform;

import javax.servlet.http.HttpSession;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class HomeController {
	
	@RequestMapping("/omikuji")
	public String home() {
		return "index.jsp";
	}
	
	@PostMapping("/process_show")
	public String show (
			@RequestParam("number") String number,
			@RequestParam("city") String city,
			@RequestParam("name") String name,
			@RequestParam("hobby") String hobby,
			@RequestParam("living_thing") String living_thing,
			@RequestParam("something_nice") String something_nice,
			HttpSession session


			) {
		System.out.println("form submitted!!!");
		
		session.setAttribute("number", number);
		session.setAttribute("city", city);
		session.setAttribute("name", name);
		session.setAttribute("hobby", hobby);
		session.setAttribute("living_thing", living_thing);
		session.setAttribute("something_nice", something_nice);
		
		
		return "redirect:/show";
		
	}
	
	@RequestMapping("/show")
	public String showForm() {
		return "showpage.jsp";
	}
	
//	@RequestMapping("/")
//	public String backToForm() {
//		return "redirect:/";
//	}
}