package com.grievance.dto;

/**
 * Data Transfer Object (DTO) for changing a user's password.
 */
public class ChangePasswordInDto {
  /**
   * oldPassword present in database.
   */
  private String oldPassword;
  /**
   * newPassword to replace oldPassword.
   */
  private String newPassword;

  /**
   * Get the old password.
   *
   * @return the old password
   */
  public String getOldPassword() {
    return oldPassword;
  }

  /**
   * Set the old password.
   *
   * @param oldPasswordField the old password to set
   */
  public void setOldPassword(final String oldPasswordField) {
    this.oldPassword = oldPasswordField;
  }

  /**
   * Get the new password.
   *
   * @return the new password
   */
  public String getNewPassword() {
    return newPassword;
  }

  /**
   * Set the new password.
   *
   * @param newPasswordField the new password to set
   */
  public void setNewPassword(final String newPasswordField) {
    this.newPassword = newPasswordField;
  }

  /**
   * Default constructor for ChangePasswordInDto.
   */
  public ChangePasswordInDto() {
  }
}