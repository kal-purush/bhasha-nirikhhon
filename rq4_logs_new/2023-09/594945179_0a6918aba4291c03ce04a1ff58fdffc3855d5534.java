package com.kipcode.blogapp.model;

public class AuthentificationResponse {
    private final String jwt;
    private final String firstName;
    private final String lastName;

    public AuthentificationResponse(String jwt,String firstName, String lastName) {
        this.jwt = jwt;
        this.firstName = firstName;
        this.lastName = lastName;
    }

    public String getJwt() {
        return jwt;
    }
    public String getFirstName(){
        return firstName;
    }
    public String getLastName(){
        return lastName;
    }
}