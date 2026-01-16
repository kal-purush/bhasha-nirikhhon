package com.spstes.model;

import java.io.Serializable;

public class PreparedCourse implements Serializable {
	private static final long serialVersionUID = -3988936121021635321L;

	private String course_name;
	private String major_name;
	private String exa_day;
	private String exa_time;
	private String exa_method;
	private String exa_num;
	private String exa_cost;

	public PreparedCourse() {
	}

	public String getCourse_name() {
		return course_name;
	}

	public void setCourse_name(String course_name) {
		this.course_name = course_name;
	}

	public String getMajor_name() {
		return major_name;
	}

	public void setMajor_name(String major_name) {
		this.major_name = major_name;
	}

	public String getExa_day() {
		return exa_day;
	}

	public void setExa_day(String exa_day) {
		this.exa_day = exa_day;
	}

	public String getExa_time() {
		return exa_time;
	}

	public void setExa_time(String exa_time) {
		this.exa_time = exa_time;
	}

	public String getExa_method() {
		return exa_method;
	}

	public void setExa_method(String exa_method) {
		this.exa_method = exa_method;
	}

	public String getExa_num() {
		return exa_num;
	}

	public void setExa_num(String exa_num) {
		this.exa_num = exa_num;
	}

	public String getExa_cost() {
		return exa_cost;
	}

	public void setExa_cost(String exa_cost) {
		this.exa_cost = exa_cost;
	}

}