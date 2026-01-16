package com.com619.group6.controllers;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.com619.group6.model.Berth;

/**
 * Rest controller to handle scheduling functions.
 * @TODO: Understand booking information requirements.
 * 
 * @author WhitearL
 */
@RestController
public class SchedulingController {

	/**
	 * Is the berth with the given ID free at the given time?
	 * Optionally accepts time. If time not supplied then will check availability at time of request.
	 * @param berthID ID of berth
	 * @param time Time to check berth availability (optional)
	 * @return Boolean indicating whether berth is free at given time.
	 */
    @GetMapping(value = "/isBerthAvailable")
    public boolean isBerthAvailable(
    		@RequestParam(required = true) int berthID,
    		@RequestParam(required = false) LocalDateTime time) {
    	
    	return true;
    }
    
    /**
     * Create a new booking
     * TODO: Get information needed to create a booking and accept as parameters.
     * @return Boolean indicating success
     */
    @GetMapping(value = "/createBooking")
    public boolean createBooking() {
    	return true;
    }

    /**
     * Update an open booking
     * @param bookingID Booking ID to update
     * TODO: Get information needed to update a booking and accept as parameters.
     * @return Boolean indicating success
     */
    @GetMapping(value = "/updateBooking")
    public boolean updateBooking(
    		@RequestParam(required = true) int bookingID) {
    	
    	return true;
    }
    
    /**
     * Cancel an open booking
     * @param bookingID Booking ID to cancel
     * @return Boolean indicating success
     */
    @GetMapping(value = "/cancelBooking")
    public boolean cancelBooking(
    		@RequestParam(required = true) int bookingID) {
    	
    	return true;
    }
       
    /**
     * @return All available berths in a list.
     */
    @GetMapping(value = "/getAvailableBerths")
    public List<Berth> getAvailableBerths() {
    	return new ArrayList<>();
    }

}