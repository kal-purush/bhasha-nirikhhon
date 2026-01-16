package se.kth.iv1201.recruitmentapplicationgroup5.controller;

import javax.validation.Valid;
import javax.validation.constraints.Email;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

public class RegistrationForm {

	@NotNull
	@Valid
	private NameDTO name;
	@NotNull
	@Email
	private String email;
	@NotNull
	@Valid
	private DateOfBirthDTO dateOfBirth;
	@NotNull
	@Size(min = 1)
	private String username;
	@NotNull
	@Size(min = 8)
	private String password;

	public RegistrationForm() {
	}

	public String getEmail() {
		return email;
	}

	public DateOfBirthDTO getDateOfBirth() {
		return dateOfBirth;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public NameDTO getName() {
		return name;
	}

}