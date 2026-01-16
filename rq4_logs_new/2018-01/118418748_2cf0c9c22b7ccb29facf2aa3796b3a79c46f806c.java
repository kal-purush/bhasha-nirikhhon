package cn.aegisa.acm.model;

import java.io.Serializable;


/**
 * City Entity.
 */
public class City implements Serializable{
	
	//列信息
	private Integer id;
	
	private String name;
	
	private String countryCode;
	
	private String district;
	
	private Integer population;
	

		
	public void setId(Integer value) {
		this.id = value;
	}
	
	public Integer getId() {
		return this.id;
	}
		
		
	public void setName(String value) {
		this.name = value;
	}
	
	public String getName() {
		return this.name;
	}
		
		
	public void setCountryCode(String value) {
		this.countryCode = value;
	}
	
	public String getCountryCode() {
		return this.countryCode;
	}
		
		
	public void setDistrict(String value) {
		this.district = value;
	}
	
	public String getDistrict() {
		return this.district;
	}
		
		
	public void setPopulation(Integer value) {
		this.population = value;
	}
	
	public Integer getPopulation() {
		return this.population;
	}
		
}
