package com.robert.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.robert.dto.CountryCitiesDTO;
import com.robert.service.PhoneBookService;
import com.robert.service.SavingService;

@RestController 
@RequestMapping("/countryCities")
public class CountryCityController {

	private final PhoneBookService phoneBookService;
	private final SavingService savingService;
	
	@Autowired
	public CountryCityController(PhoneBookService phoneBookService,
			SavingService savingService) {
		this.phoneBookService = phoneBookService;
		this.savingService = savingService;
	}
	
	@PostMapping
	public void addNewCountryCity(@RequestBody CountryCitiesDTO countryCitiesDTO) throws Exception {
		savingService.addNewCountryCityRelationship(countryCitiesDTO);
	}
}