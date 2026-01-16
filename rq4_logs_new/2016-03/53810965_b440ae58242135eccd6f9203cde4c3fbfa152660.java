/**
 * 
 */
package com.json.parse.jsonparser;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author srilu
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Address {
private int aptNo;
private String street;
private String city;
private String state;
private String country;
public int getAptNo() {
	return aptNo;
}
public void setAptNo(int aptNo) {
	this.aptNo = aptNo;
}
public String getStreet() {
	return street;
}
public void setStreet(String street) {
	this.street = street;
}
public String getCity() {
	return city;
}
public void setCity(String city) {
	this.city = city;
}
public String getState() {
	return state;
}
public void setState(String state) {
	this.state = state;
}
public String getCountry() {
	return country;
}
public void setCountry(String country) {
	this.country = country;
}
@Override
public String toString() {
	return "Address [aptNo=" + aptNo + ", street=" + street + ", city=" + city + ", state=" + state + ", country="
			+ country + "]";
}

}