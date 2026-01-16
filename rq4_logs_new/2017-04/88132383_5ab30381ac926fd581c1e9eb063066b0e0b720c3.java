package com.noteslookup.spring.test;

public class Address {
	private int id;
	private String street;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getStreet() {
		return street;
	}
	public void setStreet(String street) {
		this.street = street;
	}
	
	public void say(){
		System.out.println("hello! I'm address class");
	}
	
}