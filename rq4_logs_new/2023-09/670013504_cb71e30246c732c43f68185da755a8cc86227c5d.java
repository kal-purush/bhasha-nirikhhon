package com.grievance.dto;

import java.util.Date;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Size;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.grievance.entity.Status;
import com.grievance.entity.TicketType;

public class TicketOutWOComment {

  /**
   *minimumLength final integer.
   */
  private final int minimumLength = 1;
  /**
   *maximumLengthOfTitle final integer.
   */
  private final int maximumLengthOfTitle = 50;
  /**
   *maximumLengthOfDescription final integer.
   */
  private final int maximumLengthOfDescription = 500;

  /**
   * Integer ticketId of Ticket.
   */
  private Integer ticketId;

  /**
   * The title of the ticket.
   */
  @NotEmpty
  @Size(
    min = minimumLength,
    max = maximumLengthOfTitle,
    message = "Title too long (max 50 characters)"
  )
  private String title;

  /**
   * The type of the ticket.
   */
  @NotEmpty
  private TicketType ticketType;

  /**
   * The department associated with the ticket.
   */
  @JsonIgnore
  @JsonProperty("department")
  private String department;

  /**
   * The description of the ticket.
   */
  @NotEmpty
  @Size(
    min = minimumLength,
    max = maximumLengthOfDescription,
    message = "Description too long (max 500 characters)"
  )
  private String description;

  /**
   * The status of the ticket.
   */
  @NotEmpty
  private Status status;

  /**
   * The date when the ticket was opened.
   */
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Kolkata")
  private Date dateOpened;

  /**
   * The date when the ticket was last updated.
   */
  @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Kolkata")
  private Date lastUpdated;


  /**
   * The employee who created the ticket.
   */
  @JsonProperty("employee")
  @JsonIgnore
  private String employeeOutDto;

  /**
   * @return the ticketId
   */
  public Integer getTicketId() {
    return ticketId;
  }

  /**
   * @param ticketIdField the ticketId to set
   */
  public void setTicketId(final Integer ticketIdField) {
    this.ticketId = ticketIdField;
  }

  /**
   * @return the title
   */
  public String getTitle() {
    return title;
  }

  /**
   * @param titleField the title to set
   */
  public void setTitle(final String titleField) {
    this.title = titleField;
  }

  /**
   * @return the ticketType
   */
  public TicketType getTicketType() {
    return ticketType;
  }

  /**
   * @param ticketTypeField the ticketType to set
   */
  public void setTicketType(final TicketType ticketTypeField) {
    this.ticketType = ticketTypeField;
  }

  /**
   * @return the department
   */
  public String getDepartment() {
    return department;
  }

  /**
   * @param departmentField the department to set
   */
  public void setDepartment(final String departmentField) {
    this.department = departmentField;
  }

  /**
   * @return the description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param descriptionField the description to set
   */
  public void setDescription(final String descriptionField) {
    this.description = descriptionField;
  }

  /**
   * @return the status
   */
  public Status getStatus() {
    return status;
  }

  /**
   * @param statusField the status to set
   */
  public void setStatus(final Status statusField) {
    this.status = statusField;
  }

  /**
   * @return the dateOpened
   */
  public Date getDateOpened() {
    return dateOpened;
  }

  /**
   * @param dateOpenedField the dateOpened to set
   */
  public void setDateOpened(final Date dateOpenedField) {
    this.dateOpened = dateOpenedField;
  }

  /**
   * @return the lastUpdated
   */
  public Date getLastUpdated() {
    return lastUpdated;
  }

  /**
   * @param lastUpdatedField the lastUpdated to set
   */
  public void setLastUpdated(final Date lastUpdatedField) {
    this.lastUpdated = lastUpdatedField;
  }

/**
 * @return the employee
 */
public String getEmployee() {
return employeeOutDto;
}

/**
 * @param employeeField the employee to set
 */
public void setEmployee(final String employeeField) {
this.employeeOutDto = employeeField;
}


}