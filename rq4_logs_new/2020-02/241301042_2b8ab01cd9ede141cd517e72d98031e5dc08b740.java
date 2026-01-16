package com.trainingbasket.javaee.modal;

public class Employee {
	private int empId;
	private String empName;
	private String department;
	private float basicSalary;
	private float allowances;
	private float insDed;

	public int getEmpId() {
		return empId;
	}

	@Override
	public String toString() {
		return "Employee [empId=" + empId + ", empName=" + empName + ", department=" + department + ", basicSalary="
				+ basicSalary + ", allowances=" + allowances + ", insDed=" + insDed + ", taxDed=" + taxDed + "]";
	}

	public void setEmpId(int empId) {
		this.empId = empId;
	}

	public String getEmpName() {
		return empName;
	}

	public void setEmpName(String empName) {
		this.empName = empName;
	}

	public String getDepartment() {
		return department;
	}

	public void setDepartment(String department) {
		this.department = department;
	}

	public float getBasicSalary() {
		return basicSalary;
	}

	public void setBasicSalary(float basicSalary) {
		this.basicSalary = basicSalary;
	}

	public float getAllowances() {
		return allowances;
	}

	public void setAllowances(float allowances) {
		this.allowances = allowances;
	}

	public float getInsDed() {
		return insDed;
	}

	public void setInsDed(float insDed) {
		this.insDed = insDed;
	}

	public float getTaxDed() {
		return taxDed;
	}

	public void setTaxDed(float taxDed) {
		this.taxDed = taxDed;
	}

	private float taxDed;

	public String printableSalarySlip() {
		return "Employee Id : " + this.empId + " | Employee Name : " + this.empName
				+ "Salary Details \n--------------------------------------------------\n" + "Basic Salary : "
				+ this.basicSalary + "\nAllowances : " + this.allowances
				+ "\n--------------------------------------------------\n\t Gross : "
				+ (this.basicSalary + this.allowances)
				+ "\nDeductions :  \n--------------------------------------------------\n" + "Insurance : "
				+ this.insDed + "\nIncome Tax " + this.taxDed + "\n--------------------------------------------------\n"
				+ insDed + taxDed + "\n" + "\n\t Net Salary " + ((basicSalary + allowances) - (insDed + taxDed))+
				"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n";
	}
}