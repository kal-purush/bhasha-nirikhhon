package org.sample.controller;

import org.sample.controller.pojos.SignupForm;
import org.sample.controller.service.IUserDataService;
import org.sample.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.SessionAttributes;
import org.springframework.web.servlet.ModelAndView;

@SessionAttributes({"username"})
@Controller
public class HelpController {
	
@Autowired
IUserDataService userService;
	
	@RequestMapping(value = "/help", method = RequestMethod.GET)
	public ModelAndView help(@RequestParam(required=false,name="user") User theUser) {
		ModelAndView model = new ModelAndView("help");
    	User principal = userService.getPrincipalUser();
		User user = principal;
		model.addObject(user);
		return model;
	}
	
}