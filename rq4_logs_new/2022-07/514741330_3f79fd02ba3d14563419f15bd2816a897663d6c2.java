package com.winternship.recruiterservice.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table
public class Recruiter {

	@Id
	@GeneratedValue(strategy=GenerationType.AUTO)
	@Column(name="RECRUITER_ID")
	private long id;
	@Column(name="RECRUITER_NAME")
	private String name;
	@Column(name="COMPANY_ID")
	private long companyId;
	@Column(name="RECRUITER_YEAR")
	private int period;
	@Column(name="RECRUITER_PROFILE")
	private String profile;
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public long getCompanyId() {
		return companyId;
	}
	public void setCompanyId(long companyId) {
		this.companyId = companyId;
	}
	public int getPeriod() {
		return period;
	}
	public void setPeriod(int period) {
		this.period = period;
	}
	public String getProfile() {
		return profile;
	}
	public void setProfile(String profile) {
		this.profile = profile;
	}
}