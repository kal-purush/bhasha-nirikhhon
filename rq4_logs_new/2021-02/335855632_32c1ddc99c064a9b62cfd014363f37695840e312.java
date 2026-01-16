package com.dynamicdudes.controller;


import javax.validation.Valid;

import org.springframework.beans.propertyeditors.StringTrimmerEditor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;

import com.dynamicdudes.model.LoanApplicant;

@Controller
public class HomeController {
	
	
	@InitBinder
	public void initBinder(WebDataBinder dataBinder) {
		StringTrimmerEditor stringTrimmerEditor = new StringTrimmerEditor(true);
		dataBinder.registerCustomEditor(String.class, stringTrimmerEditor);
		
	}
	
	@RequestMapping("/")
	public String showPage()
	{
		return "Home";
	}
	
	@RequestMapping("/showform")
	public String showNewForm(Model model)
	{
		LoanApplicant loanApplication = new LoanApplicant();
		model.addAttribute("loan", loanApplication);
		return "newform";
	}
	
//	@RequestMapping("/viewform")
//	public String showExistingApplications()
//	{
//		return "viewform";
//	}
	
	@RequestMapping("/processForm2")
	public String processForm2(@Valid @ModelAttribute("loan") LoanApplicant loanApplicant , 
			BindingResult theBindingResult)
	{
		System.out.println(loanApplicant.getFirstName());
		if(theBindingResult.hasErrors())
		{
			return "newform";
		}
		else 
		{
			return "submitted";
		}
	}
}