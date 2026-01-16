package com.gslab.talent.controller;

import java.util.TreeMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gslab.talent.service.TechnologyExperienceService;

@RestController
@CrossOrigin(allowedHeaders = "*")
@RequestMapping(value="/v1")
public class TechnologyExperienceController {
	
	@Autowired
	TechnologyExperienceService techExperienceServiceObj;
	
	@GetMapping(value="/experience/{technology}")
	public ResponseEntity<TreeMap<String,Integer>> getExperienceByTechnology(@PathVariable String technology){
		System.out.println(technology);
		return new ResponseEntity<TreeMap<String,Integer>>(techExperienceServiceObj.getExperienceByTechnology(technology),HttpStatus.OK);
	}

}