package com.sweng894.GetVaccinated;

import java.sql.Date;

class Appointment {

	private String name;
	private Integer confirmationNumber;
	private String status;
	private Date date;
	
	public Appointment() {
		
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}	
	
	public Integer getConfirmationNumber() {
		return confirmationNumber;
	}

	public void setConfirmationNumber(Integer confirmationNumber) {
		this.confirmationNumber = confirmationNumber;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}
	
	
}