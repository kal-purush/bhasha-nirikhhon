package com.jackson.sample;

public class Address {

	private String Street;
	private String city;
	private String zipcode;
	
	public String getStreet() {
		return Street;
	}
	public void setStreet(String street) {
		Street = street;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getzipcode() {
		return zipcode;
	}
	public void setzipcode(String zipCode) {
		this.zipcode = zipCode;
	}
	
	@Override
	public String toString() {
		return "Street : "+getStreet() +",\nCity : "+getCity()+",\nZipCode : "+getzipcode();
	}
}