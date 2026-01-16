package com.se.fit.TravelProject.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.se.fit.TravelProject.entities.Departure;
import com.se.fit.TravelProject.entities.Destination;
import com.se.fit.TravelProject.entities.Tour;
import com.se.fit.TravelProject.service.DepartureService;
import com.se.fit.TravelProject.service.DestinationService;
import com.se.fit.TravelProject.service.TravelPackageService;

@Controller
@RequestMapping("/Tour")
public class TourController {

	private TravelPackageService packageService;
	private DepartureService departureService;
	private DestinationService destinationService;

	@Autowired
	public TourController(TravelPackageService packageService, DepartureService departureService,
			DestinationService destinationService) {
		super();
		this.packageService = packageService;
		this.departureService = departureService;
		this.destinationService = destinationService;
	}
	
	@GetMapping("/showTours")
	public String showTours(@RequestParam("departure") String departure,@RequestParam("departure")String destination,Model model) {
		List<Tour> list = packageService.getAllTours();
		model.addAttribute("LISTTOURS", list);
		return "";
	}
	
	@GetMapping("/resultSearchTour")
	public String resultSearchTour(Model model) {
		List<Departure> departures = departureService.getAllDepartures();
		List<Destination> destinations = destinationService.getAllDestinations();
		model.addAttribute("LISTDEP", departures);
		model.addAttribute("LISTDES",destinations);
		return "ResultSearchTour";
	}
}