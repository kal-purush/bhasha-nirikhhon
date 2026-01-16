package com.heningburg.newPatientReg;

public class Employer {
    private String employerName;
    private int employerContact;
    private int employerContactExt;
    private String employerStreetName;
    private int employerStreetNumber;
    private String employerState;
    private String employerCity;

//  Setters
    public void setEmployerName(String employerName) { this.employerName = employerName; }
    public void setEmployerContact(int employerContact) { this.employerContact = employerContact; }
    public void setEmployerContactExt(int employerContactExt) { this.employerContactExt = employerContactExt; }
    public void setEmployerStreetName(String employerStreetName) { this.employerStreetName = employerStreetName; }
    public void setEmployerStreetNumber(int employerStreetNumber) { this.employerStreetNumber = employerStreetNumber; }
    public void setEmployerState(String employerState) { this.employerState = employerState; }
    public void setEmployerCity(String employerCity) { this.employerCity = employerCity; }

//  Getters
    public String getEmployerName() { return employerName; }
    public int getEmployerContact() { return employerContact; }
    public int getEmployerContactExt() { return employerContactExt; }
    public String getEmployerStreetName() { return employerStreetName;}
    public int getEmployerStreetNumber() { return employerStreetNumber; }
    public String getEmployerState() { return employerState; }
    public String getEmployerCity() { return employerCity; }
}


//TODO:
// > employerNumber
// > employerZip
// > employerAddressNumber
// > employerExt
// > employerName
// > employerCity
// > employerAddress --> break down: streetName, streetNumber, zipCode,
// > employerStreetName
// > city
// > state