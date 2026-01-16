package com.vc.model;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public class HotelCity {
	
	@Id
	@GeneratedValue
	private int ID;
	private String cityName;
	private String cityId;
	private int domesticFlag;
	
	public String getCityName() {
		return cityName;
	}

	public String getCityId() {
		return cityId;
	}

	public int getDomesticFlag() {
		return domesticFlag;
	}

}