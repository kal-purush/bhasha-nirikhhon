package com.sweng894.GetVaccinated;

import java.util.Date;

public class Tier {
  private int id;
  private int vaccineId;
  private String title;
  private Date startDate;
  private Date endDate;
  private String description;

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getVaccineId() {
    return vaccineId;
  }

  public void setVaccineId(int vaccineId) {
    this.vaccineId = vaccineId;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public Date getStartDate() {
    return startDate;
  }

  public void setStartDate(Date startDate) {
    this.startDate = startDate;
  }

  public Date getEndDate() {
    return endDate;
  }

  public void setEndDate(Date endDate) {
    this.endDate = endDate;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }
}