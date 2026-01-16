package com.grievance.dto;


import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;


public class DepartmentOutDto {
  /**
   * The unique identifier for the department.
   */
  private Integer departmentId;

  /**
   * The name of the department.
   */
  private String departmentName;

  /**
   * The list of employees associated with this department.
   */
  @JsonManagedReference
  private List<EmployeeOutDto> employees;

  /**
   * The list of tickets associated with this department.
   */
  @JsonIgnore
  private List<TicketOutDto> tickets;

  /**
   * @return the departmentId
   */
  public Integer getDepartmentId() {
    return departmentId;
  }

  /**
   * @param departmentIdField the departmentId to set
   */
  public void setDepartmentId(final Integer departmentIdField) {
    this.departmentId = departmentIdField;
  }

  /**
   * @return the departmentName
   */
  public String getDepartmentName() {
    return departmentName;
  }

  /**
   * @param departmentNameField the departmentName to set
   */
  public void setDepartmentName(final String departmentNameField) {
    this.departmentName = departmentNameField;
  }

  /**
   * @return the employees
   */
  public List<EmployeeOutDto> getEmployees() {
    return employees;
  }

  /**
   * @param employeesField the employees to set
   */
  public void setEmployees(final List<EmployeeOutDto> employeesField) {
    this.employees = employeesField;
  }

  /**
   * @return the tickets
   */
  public List<TicketOutDto> getTickets() {
    return tickets;
  }

  /**
   * @param ticketsField the tickets to set
   */
  public void setTickets(final List<TicketOutDto> ticketsField) {
    this.tickets = ticketsField;
  }
}