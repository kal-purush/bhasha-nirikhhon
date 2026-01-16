package com.cognizant.authenticationservice.controller;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import com.cognizant.authenticationservice.exceptionhandling.AppUserNotFoundException;
import com.cognizant.authenticationservice.model.AppUser;
import com.cognizant.authenticationservice.model.AuthenticationResponse;
import com.cognizant.authenticationservice.repository.UserRepository;
import com.cognizant.authenticationservice.service.LoginService;
import com.cognizant.authenticationservice.service.Validationservice;

import lombok.extern.slf4j.Slf4j;


@Slf4j
@RestController
public class AuthController {

	// Users Repository
	@Autowired
	private UserRepository userRepository;

	// Service class login
	@Autowired
	private LoginService loginService;

	// Service class for login
	@Autowired
	private Validationservice validationService;

	/**
	 * @return ResponseEntity<String>
	 */
	@GetMapping("/health")
	public ResponseEntity<String> healthCheckup() {
		log.info("Health Check for Authentication Microservice");
		log.info("health checkup ----->{}", "okay");
		return new ResponseEntity<>("Okay", HttpStatus.OK);
	}

	/**
	 * @param appUserloginCredentials
	 * @return ResponseEntity<AppUser>
	 * @throws UsernameNotFoundException
	 * @throws AppUserNotFoundException
	 */
	@PostMapping("/login")
	public ResponseEntity<AppUser> login(@RequestBody AppUser appUserloginCredentials)
			throws UsernameNotFoundException, AppUserNotFoundException {
		AppUser user = loginService.userLogin(appUserloginCredentials);
		log.info("Logging in the user details");
		return new ResponseEntity<>(user, HttpStatus.ACCEPTED);
	}

	/**
	 * @param token
	 * @return AuthenticationResponse
	 */
	@GetMapping("/validateToken")
	public AuthenticationResponse getValidity(@RequestHeader("Authorization") final String token) {
		log.info("Generates JWT Token");
		return validationService.validate(token);
	}

	/**
	 * @param appUserCredentials
	 * @return ResponseEntity<?>
	 */
	@PostMapping("/createUser")
	public ResponseEntity<?> createUser(@RequestBody AppUser appUserCredentials) {
		AppUser createduser = null;
		try {
			createduser = userRepository.save(appUserCredentials);
		} catch (Exception e) {
			return new ResponseEntity<String>("Not created", HttpStatus.NOT_ACCEPTABLE);
		}
		log.info("user creation---->{}", createduser);
		return new ResponseEntity<>(createduser, HttpStatus.CREATED);

	}

	/**
	 * @param token
	 * @return ResponseEntity<List<AppUser>>
	 */
	@PreAuthorize("hasRole('ROLE_EMPLOYEE')")
	@GetMapping("/find")
	public ResponseEntity<List<AppUser>> findUsers(@RequestHeader("Authorization") final String token) {
		List<AppUser> createduser = new ArrayList<>();
		List<AppUser> findAll = userRepository.findAll();
		findAll.forEach(emp -> createduser.add(emp));
		log.info("All Users  ----->{}", findAll);
		return new ResponseEntity<>(createduser, HttpStatus.CREATED);

	}

	/**
	 * @param id
	 * @return String
	 */
	@GetMapping("/role/{id}")
	public String getRole(@PathVariable("id") String id) {
		return userRepository.findById(id).get().getRole();
	}

}