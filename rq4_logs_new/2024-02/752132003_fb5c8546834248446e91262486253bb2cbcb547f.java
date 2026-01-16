package com.web.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.web.domain.LicenseList;


@RestController
@RequestMapping("/license")
public class LicenseController {

	@Autowired
	private com.web.service.LicenseListService LicenseListService;
	
	@GetMapping("/list")
	public ResponseEntity<List<LicenseList>> getLicenses() {
        List<LicenseList> licenses = LicenseListService.getAllLicenses();
        return new ResponseEntity<>(licenses, HttpStatus.OK);
    }
	
	
	
}