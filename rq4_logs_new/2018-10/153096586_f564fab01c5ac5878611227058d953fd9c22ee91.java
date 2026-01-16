package com.experisproject.experisproject.models.entities;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import java.time.LocalDate;

@Entity
public class Person {
	@Id
	@GeneratedValue
	private int personId;

	private String firstName;
	private String lastName;
	private LocalDate dateOfBirth;

	//addressId as foreign key, reference address(addressId)


	public Person() {
	}

	public Person(String firstName, String lastName, LocalDate dateOfBirth) {
		this.firstName = firstName;
		this.lastName = lastName;
		this.dateOfBirth = dateOfBirth;
	}
}