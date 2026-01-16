package com.start.entities;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Entity
public class HOD {

	@Id
	@GeneratedValue(strategy=GenerationType.AUTO)
	private int Sid;
	private String Hname;
	public int getSid() {
		return Sid;
	}
	public void setSid(int sid) {
		Sid = sid;
	}
	public String getHname() {
		return Hname;
	}
	public void setHname(String hname) {
		Hname = hname;
	}
	public HOD() {
		super();
		// TODO Auto-generated constructor stub
	}
	public HOD(String hname) {
		super();
		Hname = hname;
	}
	
	
}