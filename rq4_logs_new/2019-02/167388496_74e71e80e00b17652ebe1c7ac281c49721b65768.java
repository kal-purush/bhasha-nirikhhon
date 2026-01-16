package com.heningburg.newPatientReg;

public class Patient {
   private String patientFirstName;
   private String patientLastName;
   private int patientHouseNumber;
   private int patientZipCode;
   private int patientSocialSecurityNum;
   private int patientBirthMonth;
   private int patientBirthDay;
   private int patientBirthYear;

//  Setters
    public void setPatientFirstName(String patientFirstName) { this.patientFirstName = patientFirstName; }
    public void setPatientLastName(String patientLastName) { this.patientLastName = patientLastName; }
    public void setPatientHouseNumber(int patientHouseNumber) { this.patientHouseNumber = patientHouseNumber; }
    public void setPatientZipCode(int patientZipCode) { this.patientZipCode = patientZipCode; }
    public void setPatientSocialSecurityNum(int patientSocialSecurityNum) { this.patientSocialSecurityNum = patientSocialSecurityNum; }
    public void setPatientBirthMonth(int patientBirthMonth) { this.patientBirthMonth = patientBirthMonth; }
    public void setPatientBirthDay(int patientBirthDay) { this.patientBirthDay = patientBirthDay; }
    public void setPatientBirthYear(int patientBirthYear) { this.patientBirthYear = patientBirthYear; }

//  Getters
    public String getPatientFirstName(){ return patientFirstName; }
    public String getPatientLastName() { return  patientLastName; }
    public int getPatientHouseNumber() { return  patientHouseNumber; }
    public int getPatientZipCode() { return patientZipCode; }
    public int getPatientSocialSecurityNum() { return patientSocialSecurityNum; }
    public int getPatientBirthMonth() { return patientBirthMonth; }
    public int getPatientBirthDay() { return patientBirthDay; }
    public int getPatientBirthYear() { return patientBirthYear; }

//    Changers
    public String changePatientFirstName(String newFirstName) {
        patientFirstName = newFirstName;
        return patientFirstName;
    }

    public String changePatientLastName(String newLastName) {
        patientLastName = newLastName;
        return patientLastName;
    }
}



//TODO:
// > patientHouseNumber
// > patientZipCode
// > patientSocialSecurityNum
// > patientBirthDate
// > employerNumber
// > employerZip
// > employerAddressNumber
// > userChoice
// > patientStreetName
// > employerName
// > employerCity
// > employerAddress
// > employerStreetName
// > city(employer)
// > state (employer)