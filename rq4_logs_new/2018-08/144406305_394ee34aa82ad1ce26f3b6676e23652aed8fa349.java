package com.cryptojobboard.cryptojobboard.Employee;


public class Employee {

    String name;
    String title;
    String image;
    String aboutMe;
    double rating;

    public Employee(String name, String title, String image, String aboutMe) {
        this.name = name;
        this.title = title;
        this.image = image;
        this.aboutMe = aboutMe;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getAboutMe() {
        return aboutMe;
    }

    public void setAboutMe(String aboutMe) {
        this.aboutMe = aboutMe;
    }
}