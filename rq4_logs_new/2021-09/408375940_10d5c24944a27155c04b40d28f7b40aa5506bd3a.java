package com.example.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.demo.dto.SpecializationDTO;
import com.example.demo.service.SpecializationService;

@RestController
@RequestMapping(path = "/specialization")
public class SpecializationController {
	SpecializationService specializationService;

	@Autowired
	public SpecializationController(SpecializationService specializationService) {
		super();
		this.specializationService = specializationService;
	}

	@PostMapping()
	public String save(@RequestBody SpecializationDTO dto) {
		specializationService.save(dto);
		return "bravo";
	}
}