package com.capstone.dentalclinic.demo.DTO;

import com.capstone.dentalclinic.demo.model.Employee;
import com.capstone.dentalclinic.demo.model.Gender;

class EmployeeDTO {

    private final Employee employee;

    EmployeeDTO(Employee employee) {
        this.employee = employee;
    }
    
    public Long getId() {
        return this.employee.getId();
    }

    public String getFirstName() {
        return this.employee.getFirstName();
    }
    
    public String getMiddleName() {
        return this.employee.getMiddleName();
    }

    public String getLastName() {
        return this.employee.getLastName();
    }

    public String getContactNumber() {
        return this.employee.getContactNumber();
    }

    public String getEmailAddress() {
        return this.employee.getEmailAddress();
    }

    public String getAddress() {
        return this.employee.getAddress();
    }

    public Gender getGender() {
        return this.employee.getGender();
    }

    public String getEmployeeFullName() {
        return getFirstName()  + getMiddleName() + getLastName();
    }
}