package com.se.fit.TravelProject.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.se.fit.TravelProject.entities.Booking;
import com.se.fit.TravelProject.service.BookingService;
import com.se.fit.TravelProject.service.TravelPackageService;

@Controller
@RequestMapping("/Booking")
public class BookingController {
	private BookingService bookingService;
	private TravelPackageService packageService;

	@Autowired
	public BookingController(BookingService bookingService, TravelPackageService packageService) {
		super();
		this.bookingService = bookingService;
		this.packageService = packageService;
	}

	@PostMapping("/saveBooking")
	public String saveBooking(@ModelAttribute("booking") Booking booking) {
		System.out.println(booking);
		bookingService.saveBooking(booking);
		return "redirect:/QLbooking/Booking";
	}

	@GetMapping("/showBooking")
	public String showBooking(Model model) {
		List<Booking> booking = bookingService.getAllBookings();
		model.addAttribute("booking", booking);
		System.out.println(booking);
		return "BookingForm";
	}



	@GetMapping("/searchBooking")
	public String searchBooking(@RequestParam("bookingId") int bookingId, Model model) {
		
		try {
			List<Booking> booking = new ArrayList<Booking>();
			booking.add(bookingService.getBookingById(bookingId));
			model.addAttribute("booking", booking);
		}catch (Exception e) {
			// TODO: handle exception
		}
		
		return "BookingForm";

	}
}