package com.se.fit.TravelProject.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.se.fit.TravelProject.entities.Booking;
import com.se.fit.TravelProject.entities.Departure;
import com.se.fit.TravelProject.entities.User;
import com.se.fit.TravelProject.service.DepartureService;

import jakarta.validation.Valid;

@Controller
@RequestMapping("/Departure")
public class DepartureController {
	private DepartureService departureService;
	
	
	@Autowired
	public DepartureController(DepartureService departureService) {
		super();
		this.departureService = departureService;
	}
	
	@PostMapping("/saveDepartute")
	public String saveDepartute(@ModelAttribute("departure") Departure departure) {
		System.out.println(departure);
		departureService.saveDeparture(departure);
		return "redirect:/Departure/showDeparture";
	}
	@GetMapping("/showDeparture")
	public String showDeparture(Model model) {
		List<Departure> departures = departureService.getAllDepartures();
		model.addAttribute("departure", departures);
		System.out.println(departures);
		return "DepartureList";
	}
	@GetMapping("/searchDeparture")
	public String searchDeparture(@RequestParam("departureId") int departureId, Model model) {
		
		try {
			List<Departure> departures = new ArrayList<Departure>();
			departures.add(departureService.getDepartureById(departureId));
			model.addAttribute("departure", departures);
		}catch (Exception e) {
			// TODO: handle exception
		}
		
		return "DepartureList";

	}
	@GetMapping("/addDeparture")
	public String  addDeparture(Model theModel) {
		Departure departure= new Departure();
		theModel.addAttribute("departure", departure);
		return "AddDepartureForm";
	
	}
	@GetMapping("/updateDeparture")
	public String updateDeparture(@RequestParam("departureId") int id, Model theModel) {
		Departure departure = departureService.getDepartureById(id);
		
		theModel.addAttribute("departure", departure);
		return "AddDepartureForm";
	}

}