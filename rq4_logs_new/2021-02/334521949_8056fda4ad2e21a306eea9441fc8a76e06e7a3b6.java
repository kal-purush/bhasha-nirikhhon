package com.sweng894.GetVaccinated.model;

import java.util.Date;

public class Vaccine {
  private int id;
  private int companyId;
  private String title;
  private Date launchDate;
  private int noOfShots;
  private String distributionProcess;

  public Vaccine() { }

  public Vaccine(int id, int companyId, String title, Date launchDate) {
    this.id = id;
    this.companyId = companyId;
    this.title = title;
    this.launchDate = launchDate;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public int getCompanyId() {
    return companyId;
  }

  public void setCompanyId(int companyId) {
    this.companyId = companyId;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public Date getLaunchDate() {
    return launchDate;
  }

  public void setLaunchDate(Date launchDate) {
    this.launchDate = launchDate;
  }

  public int getNoOfShots() {
    return noOfShots;
  }

  public void setNoOfShots(int noOfShots) {
    this.noOfShots = noOfShots;
  }

  public String getDistributionProcess() {
    return distributionProcess;
  }

  public void setDistributionProcess(String distributionProcess) {
    this.distributionProcess = distributionProcess;
  }
}