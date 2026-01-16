package se.kth.iv1201.recruitmentapplicationgroup5.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import se.kth.iv1201.recruitmentapplicationgroup5.model.Applicant;
import se.kth.iv1201.recruitmentapplicationgroup5.model.ApplicantRepository;

@RestController
@RequestMapping("/api/v1")
public class ApplicantController {

	private final ApplicantRepository repository;

	ApplicantController(ApplicantRepository repository) {
		this.repository = repository;
	}

	@PostMapping("/applicant")
	Applicant newApplicant(@RequestBody RegisterForm registerForm) {
		Applicant newApplicant = new Applicant(registerForm.getName());
		return repository.save(newApplicant);
	}

}