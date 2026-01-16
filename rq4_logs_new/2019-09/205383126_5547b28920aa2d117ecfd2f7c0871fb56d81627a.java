package org.spring.mvc.sample;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.HashMap;
import java.util.List;

public class Student {
    @Size(min = 1, message = "is required")
    private String firstName;

    @Size(min = 2, message = "is required")
    private String lastName;

    private String color;

    private List<String> genders;

    private HashMap<String, String> countries;

    @Min(value = 18)
    private Integer age;

    public Student() {
        countries = new HashMap<>();
        countries.put("MD", "Moldova");
        countries.put("BR", "Brazil");
        countries.put("US", "USA");
    }


    public HashMap<String, String> getCountries() {
        return countries;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    private String country;

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public List<String> getGenders() {
        return genders;
    }

    public void setGenders(List<String> genders) {
        this.genders = genders;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}