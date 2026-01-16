package com.se.fit.TravelProject.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.se.fit.TravelProject.entities.Combo;
import com.se.fit.TravelProject.entities.Destination;
import com.se.fit.TravelProject.service.TravelPackageService;

@Controller
@RequestMapping("/combo")
public class ComboController {
	private TravelPackageService travelPackageService;

	@Autowired
	public ComboController(TravelPackageService travelPackageService) {
		super();
		this.travelPackageService = travelPackageService;
	}
	
	@GetMapping("/detail")
	public String getAllCombo(Model model){
		Combo combo = travelPackageService.getComboById(3);
		model.addAttribute("detailCombo", combo);
		return "ComboDetail";
	}
	
}