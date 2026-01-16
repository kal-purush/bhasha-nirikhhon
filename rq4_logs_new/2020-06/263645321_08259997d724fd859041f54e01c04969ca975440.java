package dto;

import java.util.ArrayList;
import java.util.Date;

import beans.ApartmentType;

public class FilterApartment {

	private ApartmentType apartmentType;
	private Date startDate;
	private Date endDate;
	private String city;
	private Double priceMin;
	private Double priceMax;
	private Integer roomsMin;
	private Integer roomsMax;
	private Integer guestsMin;
	private Integer guestsMax;
	private ArrayList<String> amenitiesId;
	
	public FilterApartment() {
		
	}
	
	
	
	public ApartmentType getApartmentType() {
		return apartmentType;
	}

	public void setApartmentType(ApartmentType apartmentType) {
		this.apartmentType = apartmentType;
	}

	public ArrayList<String> getAmenitiesId() {
		return amenitiesId;
	}

	public void setAmenitiesId(ArrayList<String> amenitiesId) {
		this.amenitiesId = amenitiesId;
	}

	public Date getStartDate() {
		return startDate;
	}
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}
	public Date getEndDate() {
		return endDate;
	}
	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public Double getPriceMin() {
		return priceMin;
	}
	public void setPriceMin(Double priceMin) {
		this.priceMin = priceMin;
	}
	public Double getPriceMax() {
		return priceMax;
	}
	public void setPriceMax(Double priceMax) {
		this.priceMax = priceMax;
	}
	public Integer getRoomsMin() {
		return roomsMin;
	}
	public void setRoomsMin(Integer roomsMin) {
		this.roomsMin = roomsMin;
	}
	public Integer getRoomsMax() {
		return roomsMax;
	}
	public void setRoomsMax(Integer roomsMax) {
		this.roomsMax = roomsMax;
	}
	public Integer getGuestsMin() {
		return guestsMin;
	}
	public void setGuestsMin(Integer guestsMin) {
		this.guestsMin = guestsMin;
	}
	public Integer getGuestsMax() {
		return guestsMax;
	}
	public void setGuestsMax(Integer guestsMax) {
		this.guestsMax = guestsMax;
	}
	
	
}