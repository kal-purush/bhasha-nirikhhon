package com.vc.model;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

@Entity
public class User
{
  @Id
  @GeneratedValue
  private int ID;
  private String userid;
  private String firstname;
  private String lastname;
  private String email;
  private String middlename;
  private String dob;
  private String gender;
  private String cuisines;
  private String activities;
  private String placesofinterest;
  
  public int getID()
  {
    return this.ID;
  }
  
  public void setID(int iD)
  {
    this.ID = iD;
  }
  
  public String getUserid()
  {
    return this.userid;
  }
  
  public void setUserid(String userid)
  {
    this.userid = userid;
  }
  
  public String getFirstname()
  {
    return this.firstname;
  }
  
  public void setFirstname(String firstname)
  {
    this.firstname = firstname;
  }
  
  public String getLastname()
  {
    return this.lastname;
  }
  
  public void setLastname(String lastname)
  {
    this.lastname = lastname;
  }
  
  public String getEmail()
  {
    return this.email;
  }
  
  public void setEmail(String email)
  {
    this.email = email;
  }
  
  public String getMiddlename()
  {
    return this.middlename;
  }
  
  public void setMiddlename(String middlename)
  {
    this.middlename = middlename;
  }
  
  public String getDob()
  {
    return this.dob;
  }
  
  public void setDob(String dob)
  {
    this.dob = dob;
  }
  
  public String getGender()
  {
    return this.gender;
  }
  
  public void setGender(String gender)
  {
    this.gender = gender;
  }
  
  public String getCuisines()
  {
    return this.cuisines;
  }
  
  public void setCuisines(String cuisines)
  {
    this.cuisines = cuisines;
  }
  
  public String getActivities()
  {
    return this.activities;
  }
  
  public void setActivities(String activities)
  {
    this.activities = activities;
  }
  
  public String getPlacesofinterest()
  {
    return this.placesofinterest;
  }
  
  public void setPlacesofinterest(String placesofinterest)
  {
    this.placesofinterest = placesofinterest;
  }
}