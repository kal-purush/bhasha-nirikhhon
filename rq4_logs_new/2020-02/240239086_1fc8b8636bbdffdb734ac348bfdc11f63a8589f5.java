package com.airport.mgmt.controller;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.logout.SecurityContextLogoutHandler;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.airport.mgmt.form.User;

@Controller
public class LoginController {

	private static Logger log=Logger.getLogger(LoginController.class);
	
	@RequestMapping(value="/login")
	public String loginPage(@RequestParam(value = "error", required = false) String error, 
			@RequestParam(value = "logout", required = false) String logout, 
			Map<String,Object> map)
	{
		
		log.info("Inside login :"+error);
		if(error != null) {
			map.put("errorMessage", "Invalid credentials");
		}
		if(logout != null) {
			map.put("errorMessage", "You have been successfully logged out !!");
        }
		return "login";
	}
	
	@RequestMapping(value="/logout", method = RequestMethod.GET)
    public String logoutPage (HttpServletRequest request, HttpServletResponse response) {
		log.info("Inside logout");
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth != null){    
            new SecurityContextLogoutHandler().logout(request, response, auth);
        }
        return "redirect:/login?logout=true";
    }
	
	@RequestMapping(value="/register")
	public String userRegister(Map<String,Object> map)
	{
		map.put("user", new User());
		//map.put("planeList", planeService.listPlane());
		log.info("Inside login  register");
		return "userregister";
	}
	
	@RequestMapping(value="/api/index")
	public String homePage(Map<String,Object> map)
	{
		log.info("Inside index");
		return "index";
	}
	
}