package com.ht.bean;


public class Student {
    private Integer no;
    private String name;
    private String gender;
    private String phone;

    public Student() {

    }

    public Student(Integer no, String name, String gender, String phone) {
        this.no = no;
        this.name = name;
        this.gender = gender;
        this.phone = phone;
    }

    public Integer getNo() {
        return no;
    }

    public void setNo(Integer no) {
        this.no = no;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }
}