package controller;

import java.util.ArrayList;
import java.util.List;

import javax.faces.bean.ApplicationScoped;
import javax.faces.bean.ManagedBean;

import dao.EmployeeDaoImpl;
import model.EmployeeDetails;

@ManagedBean
@ApplicationScoped
public class EmployeeBean {
	EmployeeDetails employeeDetails = new EmployeeDetails();
	EmployeeDaoImpl employeeDaoImpl = new EmployeeDaoImpl();
	private EmployeeDetails updateDetails = new EmployeeDetails();
	List<EmployeeDetails> list;

	public EmployeeBean() {

		list = new ArrayList<EmployeeDetails>();
		list = employeeDaoImpl.showAllEmployees();
	}

	public EmployeeDetails getEmployeeDetails() {
		return employeeDetails;
	}

	public void setEmployeeDetails(EmployeeDetails employeeDetails) {
		this.employeeDetails = employeeDetails;
	}

	public EmployeeDaoImpl getEmployeeDaoImpl() {
		return employeeDaoImpl;
	}

	public void setEmployeeDaoImpl(EmployeeDaoImpl employeeDaoImpl) {
		this.employeeDaoImpl = employeeDaoImpl;
	}

	public List<EmployeeDetails> getList() {
		return list;
	}

	public void setList(List<EmployeeDetails> list) {
		this.list = list;
	}

	public EmployeeDetails getUpdateDetails() {
		return updateDetails;
	}

	public void setUpdateDetails(EmployeeDetails updateDetails) {
		this.updateDetails = updateDetails;
	}

	public String save() {
		list.add(employeeDetails);
		employeeDaoImpl.saveEmployee(employeeDetails);
		return "employee?faces-redirect=true";
	}

	public void delete(EmployeeDetails details) {
		employeeDaoImpl.deleteEmployee(details);
		list = employeeDaoImpl.showAllEmployees();
	}

	public void update(int id) {
		savechange();
		updateDetails.setId(id);
		employeeDaoImpl.updateEmployee(updateDetails);
		list=employeeDaoImpl.showAllEmployees();
	}

	public void updatability(EmployeeDetails details) {
		details.setUpd(true);
	}

	public void savechange() {
		for (EmployeeDetails details : list) {
			details.setUpd(false);
		}
	}

}