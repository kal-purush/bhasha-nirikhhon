package com.emst.Fudocs.controller;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.emst.Fudocs.dto.AcademicYearDTO;
import com.emst.Fudocs.dto.CriteriaOneNaacDTO;
import com.emst.Fudocs.service.FirstCriteriaService;
import com.emst.Fudocs.service.MasterService;

@RestController
@RequestMapping("/criteria1")
public class FirstCriteriaController {
private Logger log = LoggerFactory.getLogger(MasterScreenController.class);
	
	@Autowired
	private FirstCriteriaService criteriaService;
	
	@PostMapping("/addCriteria")
	public ResponseEntity<CriteriaOneNaacDTO> AddCriteria(@RequestBody CriteriaOneNaacDTO firstCriteria){
		log.info("inside  AddCriteria");
		
		CriteriaOneNaacDTO acYear=criteriaService.addCriteria(firstCriteria);
		return ResponseEntity.ok().body(acYear);
		
	}
	
	@GetMapping("/getCriteria")
	public ResponseEntity<List<CriteriaOneNaacDTO>> getCriteria(){
		log.info("inside getEmployee() in EmployeeController class");
		List<CriteriaOneNaacDTO> emp = criteriaService.findAllCriteria();
		return new ResponseEntity<>(emp, HttpStatus.OK);
	}
	
	@PostMapping("/updateAcademicYear")
	public ResponseEntity<CriteriaOneNaacDTO> updateCriteria(@RequestBody CriteriaOneNaacDTO academicYear){
		log.info("inside addacademicyear");
		
		CriteriaOneNaacDTO acYear=criteriaService.updateCriteria(academicYear);
		return ResponseEntity.ok().body(acYear);
		
	}
	@DeleteMapping("/deleteCriteria/{id}")
	public ResponseEntity<?> deleteAcademicYear(@PathVariable Long id){
		log.info("inside add deleteAcademicYear");
		
		criteriaService.deleteCriteriaById(id);
		return new ResponseEntity<>(HttpStatus.OK);
		
	}
}