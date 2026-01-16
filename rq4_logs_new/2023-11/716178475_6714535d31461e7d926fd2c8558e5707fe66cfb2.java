package com.se.fit.TravelProject.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.se.fit.TravelProject.entities.Combo;
import com.se.fit.TravelProject.entities.Tour;
import com.se.fit.TravelProject.service.TravelPackageService;

@RestController
@RequestMapping("/travel")
public class TravelPackageController {
	private TravelPackageService packageService;

	@Autowired
	public TravelPackageController(TravelPackageService packageService) {
		super();
		this.packageService = packageService;
	}
	
	@GetMapping("/showTours")
	public List<Tour> showTours(){
		return packageService.getAllTours();
	}
	
	@GetMapping("/showCombos")
	public List<Combo> showCombos(){
		return packageService.getAllCombos();
	}
}