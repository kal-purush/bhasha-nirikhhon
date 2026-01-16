package com.qa.web;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DepartureAirport {
	
    private String name;
    private String weatherZone;
    private String countryName;
    private String stateCode;
    
	public String getName() {
		return name;
	}
	public String getWeatherZone() {
		return weatherZone;
	}	
	public String getCountryName() {
		return countryName;
	}
	public String getStateCode() {
		return stateCode;
	}
     
}