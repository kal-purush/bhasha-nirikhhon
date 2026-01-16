package com.dss.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class pagecontroller {

	@RequestMapping({"/" , "/home","/index"})
	public ModelAndView index()
	{
		ModelAndView mv=new ModelAndView("page");
		
		mv.addObject("greeting", "Welcome to SpringMVC");
		return mv;	
	}
	
	/*//Understanding @RequesParam
	@RequestMapping("/test")
	public ModelAndView test(@RequestParam(value="greeting",required=false) 	
	{
		if(greeting==null)
		{
			greeting="hello there";
		}
		ModelAndView mv=new ModelAndView("page");
		
		mv.addObject("greeting", "Welcome to SpringMVC");
		return mv;	
	}*/
	
	@RequestMapping(value="/test/{greeting}")
	public ModelAndView test(@PathVariable("greeting") String greeting	)
	{
		if(greeting==null)
		{
			greeting="hello there";
		}
		ModelAndView mv=new ModelAndView("page");
		
		mv.addObject("greeting", greeting);
		return mv;	
	}
}