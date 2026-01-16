package com.spstes.model;

import java.io.Serializable;
import java.math.BigInteger;

public class TestTime implements Serializable {

	private static final long serialVersionUID = 4767937954827249465L;

	private BigInteger exa_num;
	private String Estart_time; // 此处使用String类型，数据库使用的是SQL中的datetime类型
	private String Estop_time; // 此处使用String类型，数据库使用的是SQL中的datetime类型
	private String freeze;
	private String Bstart_time; // 此处使用String类型，数据库使用的是SQL中的datetime类型
	private String Bend_time; // 此处使用String类型，数据库使用的是SQL中的datetime类型

	public TestTime() {
		super();
	}

	public BigInteger getExa_num() {
		return exa_num;
	}

	public void setExa_num(BigInteger exa_num) {
		this.exa_num = exa_num;
	}

	public String getEstart_time() {
		return Estart_time;
	}

	public void setEstart_time(String estart_time) {
		Estart_time = estart_time;
	}

	public String getEstop_time() {
		return Estop_time;
	}

	public void setEstop_time(String estop_time) {
		Estop_time = estop_time;
	}

	public String getFreeze() {
		return freeze;
	}

	public void setFreeze(String freeze) {
		this.freeze = freeze;
	}

	public String getBstart_time() {
		return Bstart_time;
	}

	public void setBstart_time(String bstart_time) {
		Bstart_time = bstart_time;
	}

	public String getBend_time() {
		return Bend_time;
	}

	public void setBend_time(String bend_time) {
		Bend_time = bend_time;
	}

}