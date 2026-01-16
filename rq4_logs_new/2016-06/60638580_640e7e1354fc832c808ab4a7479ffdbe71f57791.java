package com.frracm.asclepio.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.frracm.asclepio.model.nif.NIF;

public class Patient {
	private String nombre;
	private String apellido1;
	private String apellido2;
	private Long numeroHistoriaClinica;
	private NIF identityDocument;
	private Date birthDate;
	private String socialSecurityNumber;
	private String birthPlace;
	private Sex sex;
	// private BloodType bloodType;// TODO
	// private Cobertura coberturaType;// TODO
	private List<String> phoneNumbers;
	private String nationality;
	private String address;

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public String getApellido1() {
		return apellido1;
	}

	public void setApellido1(String apellido1) {
		this.apellido1 = apellido1;
	}

	public String getApellido2() {
		return apellido2;
	}

	public void setApellido2(String apellido2) {
		this.apellido2 = apellido2;
	}

	public Long getNumeroHistoriaClinica() {
		return numeroHistoriaClinica;
	}

	public void setNumeroHistoriaClinica(Long numeroHistoriaClinica) {
		this.numeroHistoriaClinica = numeroHistoriaClinica;
	}

	public NIF getIdentityDocument() {
		return identityDocument;
	}

	public void setIdentityDocument(NIF identityDocument) {
		this.identityDocument = identityDocument;
	}

	public Date getBirthDate() {
		return birthDate == null ? null : new Date(birthDate.getTime());
	}

	public void setBirthDate(Date birthDate) {
		this.birthDate = birthDate == null ? null : new Date(birthDate.getTime());
	}

	public String getSocialSecurityNumber() {
		return socialSecurityNumber;
	}

	public void setSocialSecurityNumber(String socialSecurityNumber) {
		this.socialSecurityNumber = socialSecurityNumber;
	}

	public String getBirthPlace() {
		return birthPlace;
	}

	public void setBirthPlace(String birthPlace) {
		this.birthPlace = birthPlace;
	}

	public Sex getSex() {
		return sex;
	}

	public void setSex(Sex sex) {
		this.sex = sex;
	}

	public List<String> getPhoneNumbers() {
		return phoneNumbers == null ? null : new ArrayList<String>(phoneNumbers);
	}

	public void setPhoneNumbers(List<String> phoneNumbers) {
		this.phoneNumbers = phoneNumbers == null ? null : new ArrayList<String>(phoneNumbers);
	}
	
	public String getNationality() {
		return nationality;
	}

	public void setNationality(String nationality) {
		this.nationality = nationality;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

}