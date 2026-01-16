package com.ddogdog.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.ddogdog.model.dto.Pet;
import com.ddogdog.service.PetService;

@RestController
@RequestMapping("pet")
public class PetController {
	private PetService petService;
	
	public PetController(PetService petService) {
		this.petService = petService;
	}
	
	@PostMapping("")
	public ResponseEntity<?> createPet(@ModelAttribute Pet pet) {
		boolean result = petService.create(pet);
		if(result)
			return ResponseEntity.ok().build();
		return ResponseEntity.badRequest().build();
	}
	
	
}