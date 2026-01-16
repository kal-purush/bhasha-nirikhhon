package com.university.contractors.controller.payload;

import com.university.contractors.model.*;

import java.util.Date;

public class SearchStudent {

    private String surname;
    private String name;
    private Date dataOfBirth;
    private Nationality nationality;
    private Country country;
    private String contractNumber;
    private Integer course;
    private Faculty faculty;
    private Direction direction;
    private EducationProgram educationProgram;
    private EducationLevel educationLevel;
    private EducationForm educationForm;
    private ArrivalLine arrivalLine;
    private EducationLanguage educationLanguage;

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getDataOfBirth() {
        return dataOfBirth;
    }

    public void setDataOfBirth(Date dataOfBirth) {
        this.dataOfBirth = dataOfBirth;
    }

    public Nationality getNationality() {
        return nationality;
    }

    public void setNationality(Nationality nationality) {
        this.nationality = nationality;
    }

    public Country getCountry() {
        return country;
    }

    public void setCountry(Country country) {
        this.country = country;
    }

    public String getContractNumber() {
        return contractNumber;
    }

    public void setContractNumber(String contractNumber) {
        this.contractNumber = contractNumber;
    }

    public Integer getCourse() {
        return course;
    }

    public void setCourse(Integer course) {
        this.course = course;
    }

    public Faculty getFaculty() {
        return faculty;
    }

    public void setFaculty(Faculty faculty) {
        this.faculty = faculty;
    }

    public Direction getDirection() {
        return direction;
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public EducationProgram getEducationProgram() {
        return educationProgram;
    }

    public void setEducationProgram(EducationProgram educationProgram) {
        this.educationProgram = educationProgram;
    }

    public EducationLevel getEducationLevel() {
        return educationLevel;
    }

    public void setEducationLevel(EducationLevel educationLevel) {
        this.educationLevel = educationLevel;
    }

    public EducationForm getEducationForm() {
        return educationForm;
    }

    public void setEducationForm(EducationForm educationForm) {
        this.educationForm = educationForm;
    }

    public ArrivalLine getArrivalLine() {
        return arrivalLine;
    }

    public void setArrivalLine(ArrivalLine arrivalLine) {
        this.arrivalLine = arrivalLine;
    }

    public EducationLanguage getEducationLanguage() {
        return educationLanguage;
    }

    public void setEducationLanguage(EducationLanguage educationLanguage) {
        this.educationLanguage = educationLanguage;
    }

    public enum Nationality {
        UKRAINIAN, FOREIGN
    }
}