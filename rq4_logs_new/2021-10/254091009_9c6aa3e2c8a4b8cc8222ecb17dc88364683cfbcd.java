package com.mudit.demo.model;

import java.util.Collection;
import java.util.List;

import javax.persistence.*;
import javax.persistence.OneToMany;

@Entity
public class Branches {
	
	@Id
	private int branchId;

	private String branchName;

	@OneToMany(mappedBy = "branchId")
	private List<Employee> employeeCollection;

	public int getBranchId() {
		return branchId;
	}

	public void setBranchId(int branchId) {
		this.branchId = branchId;
	}

	public String getBranchName() {
		return branchName;
	}

	public void setDeptName(String branchName) {
		this.branchName = branchName;
	}

	public List<Employee> getEmployeeCollection() {
		return employeeCollection;
	}

	public void setEmployeeCollection(List<Employee> employeeCollection) {
		this.employeeCollection = employeeCollection;
	}
		
	
	
	
}