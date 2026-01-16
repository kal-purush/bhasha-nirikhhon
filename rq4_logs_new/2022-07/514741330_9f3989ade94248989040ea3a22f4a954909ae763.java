package com.winternship.recruiterservice.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.winternship.recruiterservice.entity.Recruiter;
import com.winternship.recruiterservice.repo.RecruiterRepository;

@RestController
@RequestMapping("/recruiter")
public class RecruiterWebService {

	private final RecruiterRepository recruiterRepo;
	
	public RecruiterWebService(RecruiterRepository recruiterRepo) {
		this.recruiterRepo = recruiterRepo;
	}
	
	@GetMapping
	public Iterable<Recruiter> getAll() {
		return this.recruiterRepo.findAll();
	}
	
	@GetMapping("/{id}")
	public Recruiter getRecruiter(@PathVariable long id) {
		return this.recruiterRepo.findById(id).get();
	}
	
}