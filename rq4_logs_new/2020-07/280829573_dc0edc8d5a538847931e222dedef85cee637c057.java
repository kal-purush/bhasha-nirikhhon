package com.LinkeT.LinkeT;

import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Handles requests for the application home page.
 */
@Controller
public class HomeController {
	
	private static final Logger logger = LoggerFactory.getLogger(HomeController.class);
	
	/**
	 * Simply selects the home view to render by returning its name.
	 */
	@RequestMapping(value = "/", method = RequestMethod.GET)
	public String home(Locale locale, Model model) {
		logger.info("Welcome home! The client locale is {}.", locale);
		
		Date date = new Date();
		DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG, locale);
		
		/* String formattedDate = dateFormat.format(date); */
		
		/* model.addAttribute("serverTime", formattedDate ); */
		
		return "redirect:/r/Login.html";
	}
	
	/*
	 * @RequestMapping(value="/main", method=RequestMethod.GET) public String
	 * main(Locale locale, Model model, HttpServletRequest request, HttpSession
	 * session) { return "forward:/r/Login.html"; }
	 */
	
	@RequestMapping(value="/sresolve", method=RequestMethod.GET)
	@ResponseBody
	public HashMap<String,String> srevole(Model model, HttpServletRequest request, HttpSession session) {
		HashMap<String, String> result= new HashMap<String,String>();
		String usrId = (String)session.getAttribute("sessionKey");
		logger.info("Log from : /sresolve :: " + usrId + "Resolved his session");
		result.put("usrId", usrId);
		
		return result;
	}
}