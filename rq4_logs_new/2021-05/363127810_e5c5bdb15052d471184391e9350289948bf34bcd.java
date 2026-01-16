package com.start.entities;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Data;


@Entity
@Table(name="staff") 
public class Staff{

	
	@Id
	@GeneratedValue(strategy=GenerationType.AUTO)
	private int hId;
	private String facultyName;
	private String facultyDept;
	
	
	public Staff() {
		super();
		// TODO Auto-generated constructor stub
	}
	public Staff(String facultyName, String facultyDept) {
		super();
		this.facultyName = facultyName;
		this.facultyDept = facultyDept;
	}
	public int gethId() {
		return hId;
	}
	public void sethId(int hId) {
		this.hId = hId;
	}
	public String getFacultyName() {
		return facultyName;
	}
	public void setFacultyName(String facultyName) {
		this.facultyName = facultyName;
	}
	public String getFacultyDept() {
		return facultyDept;
	}
	public void setFacultyDept(String facultyDept) {
		this.facultyDept = facultyDept;
	}
		
	
	
}