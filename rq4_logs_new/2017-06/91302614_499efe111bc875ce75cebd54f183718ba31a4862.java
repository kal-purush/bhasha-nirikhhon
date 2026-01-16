package com.quick.location.entity.old;

import java.io.Serializable;
import javax.persistence.*;


/**
 * The persistent class for the openinghours database table.
 * 
 */
//@Entity
//@Table(name="openinghours")
@NamedQuery(name="OpeninghourEntity.findAll", query="SELECT o FROM OpeninghourEntity o")
public class OpeninghourEntity implements Serializable {
	private static final long serialVersionUID = 1L;


	@GeneratedValue(strategy=GenerationType.AUTO)
	private int idopeningHours;
	
	@Id
	@Column(name="place_id")
	private String placeId;

	@Column(name="weekday_text")
	private String weekdayText;

	public OpeninghourEntity() {
	}

	public int getIdopeningHours() {
		return this.idopeningHours;
	}

	public void setIdopeningHours(int idopeningHours) {
		this.idopeningHours = idopeningHours;
	}

	public String getPlaceId() {
		return this.placeId;
	}

	public void setPlaceId(String placeId) {
		this.placeId = placeId;
	}

	public String getWeekdayText() {
		return this.weekdayText;
	}

	public void setWeekdayText(String weekdayText) {
		this.weekdayText = weekdayText;
	}

}