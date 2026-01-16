package com.franciszabala.simplewebservice.model;

import java.math.BigDecimal;

import org.apache.commons.lang3.builder.CompareToBuilder;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;
import lombok.Setter;

public class PlanSimple implements Comparable<PlanSimple> {

	public PlanSimple(Plan p) {
		super();

		this.planName = p.getPlanName();
		this.planPrice = p.getPlanPrice();
		this.expiry = p.getExpiry();
		this._plan = p.isPlan();
		this._unlimited = p.isUnlimited();
		this.sizeMb = p.getSizeMb();
		this._4g = p.is4g();
		this._autoRenew = p.isAutoRenew();
		this.termsUrl = p.getTermsUrl();
		this.infoUrl = p.getInfoUrl();
	}

	/*
	@JsonProperty("plan_id")
	@Getter @Setter private String planId;
	*/

	@JsonProperty("plan_name")
	@Getter @Setter private String planName;
	

	@JsonProperty("plan_price")
	@Getter @Setter private BigDecimal planPrice;
	

	@JsonProperty("expiry")
	@Getter @Setter private Integer expiry;
	

	@JsonProperty("is_plan")
	private Boolean _plan;
	

	@JsonProperty("is_unlimited")
	private Boolean _unlimited;
	

	@JsonProperty("size_mb")
	@Getter @Setter private Integer sizeMb;
	
	private Boolean _4g;
	
	private Boolean _autoRenew;
	

	@JsonProperty("terms_url")
	@Getter @Setter private String termsUrl;
	

	@JsonProperty("info_url")
	@Getter @Setter private String infoUrl;

	@JsonGetter("is_plan")
	public Boolean isPlan() {
		return _plan;
	}

	public void setPlan(Boolean _plan) {
		this._plan = _plan;
	}

	@JsonGetter("is_unlimited")
	public Boolean isUnlimited() {
		return _unlimited;
	}

	public void setUnlimited(Boolean _unlimited) {
		this._unlimited = _unlimited;
	}

	@JsonGetter("is_4g")
	public Boolean is4g() {
		return _4g;
	}

	public void set4g(Boolean _4g) {
		this._4g = _4g;
	}

	@JsonGetter("is_auto_renew")
	public Boolean isAutoRenew() {
		return _autoRenew;
	}

	public void setAutoRenew(Boolean _autoRenew) {
		this._autoRenew = _autoRenew;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((planName == null) ? 0 : planName.hashCode());
		result = prime * result + ((sizeMb == null) ? 0 : sizeMb.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PlanSimple other = (PlanSimple) obj;
		if (planName == null) {
			if (other.planName != null)
				return false;
		} else if (!planName.equals(other.planName))
			return false;
		if (sizeMb == null) {
			if (other.sizeMb != null)
				return false;
		} else if (!sizeMb.equals(other.sizeMb))
			return false;
		return true;
	}
	
	@Override
	public int compareTo(PlanSimple o) {
		CompareToBuilder compareToBuilder = new CompareToBuilder();
		compareToBuilder.append(sizeMb, o.sizeMb);
		return compareToBuilder.toComparison();
	}
}