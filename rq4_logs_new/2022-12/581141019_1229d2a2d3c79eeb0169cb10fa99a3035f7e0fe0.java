package com.ems.ThymeLeaf.entity;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;

@Entity
@Table(name="tbluser")
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private int userId;
    @Column
    private String fName;
    @Column
    private String lName;
    @Column
    private int age;
    @Column
    private String currentAddress;
    @Column
    private String permanentAddress;
    @Column
    private String gender;
    @Column
    private double salary;
    @Column
    private long contactInfo;
    @OneToOne(cascade = CascadeType.ALL,fetch = FetchType.EAGER)
    @JoinColumn(name="empId")
    private Employee employee;
    
    public User() {
        super();
        // TODO Auto-generated constructor stub
    }

    public User(String fName, String lName, int age, String currentAddress, String permanentAddress,
            String gender, double salary, long contactInfo, Employee employee) {
        super();
        this.fName = fName;
        this.lName = lName;
        this.age = age;
        this.currentAddress = currentAddress;
        this.permanentAddress = permanentAddress;
        this.gender = gender;
        this.salary = salary;
        this.contactInfo = contactInfo;
        this.employee = employee;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getfName() {
        return fName;
    }

    public void setfName(String fName) {
        this.fName = fName;
    }

    public String getlName() {
        return lName;
    }

    public void setlName(String lName) {
        this.lName = lName;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getCurrentAddress() {
        return currentAddress;
    }

    public void setCurrentAddress(String currentAddress) {
        this.currentAddress = currentAddress;
    }

    public String getPermanentAddress() {
        return permanentAddress;
    }

    public void setPermanentAddress(String permanentAddress) {
        this.permanentAddress = permanentAddress;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    public long getContactInfo() {
        return contactInfo;
    }

    public void setContactInfo(long contactInfo) {
        this.contactInfo = contactInfo;
    }

    public Employee getEmployee() {
        return employee;
    }

    public void setEmployee(Employee employee) {
        this.employee = employee;
    }

    @Override
    public String toString() {
        return "User [userId=" + userId + ", fName=" + fName + ", lName=" + lName + ", age=" + age + ", currentAddress="
                + currentAddress + ", permanentAddress=" + permanentAddress + ", gender=" + gender + ", salary="
                + salary + ", contactInfo=" + contactInfo + ", employee=" + employee + "]";
    }
    
    
}