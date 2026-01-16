package com.grievance.dto;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class CommentOutDto {

  /**
   * description of comment.
   */
  private String description;

  /**
   * comment belong to this userName.
   */
  private String userName;

  /**
   * comment created on date.
   */
  @JsonFormat(pattern = "dd-MM-yyyy hh:mm:ss aa", timezone = "Asia/Kolkata")
  private Date createdOn;

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
   * @return the userName
   */
  public String getUserName() {
    return userName;
  }

  /**
   * @param userNameField the userName to set
   */
  public void setUserName(final String userNameField) {
    this.userName = userNameField;
  }

  /**
   * @return the createdOn
   */
  public Date getCreatedOn() {
    return createdOn;
  }

  /**
   * @param createdOnField the createdOn to set
   */
  public void setCreatedOn(final Date createdOnField) {
    this.createdOn = createdOnField;
  }
}