package net.google.journalApp.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import net.google.journalApp.entity.GenerateOtp;
import net.google.journalApp.exception.ResourceNotFoundException;
import net.google.journalApp.exception.ServiceResponse;
import net.google.journalApp.service.GenerateOtpService;

@RestController
@RequestMapping("v1/generate-otp")
public class GenerateOtpController {

	@Autowired
	private GenerateOtpService generateOtpService;

	@PostMapping
	public ServiceResponse saveGenerateOtp(@RequestBody GenerateOtp generateOtp) {

		return ServiceResponse.asSuccess(generateOtpService.saveGenerateOtp(generateOtp));

	}

	@GetMapping
	public ServiceResponse getAllGenerateOtpByUser() {

		return ServiceResponse.asSuccess(generateOtpService.getAllGenerateOtpByUser());

	}

	@GetMapping("/{id}")
	public ServiceResponse GenerateOtpById(@PathVariable("id") String id) throws ResourceNotFoundException {

		return ServiceResponse.asSuccess(generateOtpService.GenerateOtpById(id));

	}

	@DeleteMapping("/{id}")
	public ServiceResponse deleteGenerateOtpById(@PathVariable("id") String id) {

		return ServiceResponse.asSuccess(generateOtpService.deleteGenerateOtpById(id));

	}

}