package com.qa.dvdLibrary.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.qa.dvdLibrary.entity.Dvd;
import com.qa.dvdLibrary.service.DvdService;

@RestController
public class Controller {
	
	private DvdService service; //link to service class
	
	@Autowired
	public Controller(DvdService service) {
	this.service = service;
	}
	
    //  Create a greeting so user can select which service they want
	@GetMapping("/greeting")
	public String greeting() {
        return "Welcome to the DVD LIbrary. What would you like to do?";
	}
	
	// This is the Create function to add data to a database
	@PostMapping("/create")
	public ResponseEntity <Dvd> createDVD(@RequestBody Dvd dvd) {
		return new ResponseEntity<Dvd>(this.service.createDvd(dvd), HttpStatus.CREATED);
		
	}
	
}