// Copyright (c) 2017-2018 LetUs Learn Inc.
package org.letuslearn.beans.data;

/**
 * 
 * @author aksharaaaa Get the User data.
 *
 */
public class User {
  // private String userName;
  private int userId;
  private String studentName;
  private int age;
  private String gender;
  private String firstName;

  public User(int userId, String firstName, String lastName, String studentName, String userName,
              String email, int age, String gender, String password){
    this.userId = userId;
    this.studentName = studentName;
    this.age = age;
    this.gender = gender;
    this.firstName = firstName;
    this.lastName = lastName;
    this.userName = userName;
    this.email = email;
    this.password = password;
  }
  public String getUserName() {
    return userName;
  }


  private String userName;
  public String getFirstName() {
    return firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public String getEmail() {
    return email;
  }

  public String getPassword() {
    return password;
  }

  private String lastName;
  private String email;
  private String password;
  /*
   * public String getUserName() { return userName; } public void setUserName(String userName) {
   * this.userName = userName; }
   */
  public String getStudentName() {
    return studentName;
  }

  public int getUserId() {
    return userId;
  }

  public int getAge() {
    return age;
  }

  public String getGender() {
    return gender;
  }
  @Override
  public String toString(){
    return String.format("User Id: '" + this.userId + "' First Name: '" + this.firstName + "' Last Name: '"+this.lastName);
  }

}