package com.example.classroommanagement;

public class studentmodel
{
    private String Name;
    private String EnrollmentNumber;
    private String Phone;
    private String Email;

    public studentmodel(){}

    private studentmodel(String studentname, String studentenroll, String studentphone, String studentemail)
    {
        this.Name = studentname;
        this.EnrollmentNumber = studentenroll;
        this.Phone = studentphone;
        this.Email = studentemail;
    }


    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public String getEnrollmentNumber() {
        return EnrollmentNumber;
    }

    public void setEnrollmentNumber(String enrollmentNumber) {
        EnrollmentNumber = enrollmentNumber;
    }

    public String getPhone() {
        return Phone;
    }

    public void setPhone(String phone) {
        Phone = phone;
    }

    public String getEmail() {
        return Email;
    }

    public void setEmail(String email) {
        Email = email;
    }
}