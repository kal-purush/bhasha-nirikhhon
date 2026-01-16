package com.example.firebasegsocapp.domain;

public class User {

    private String IdToken;
    private String email;
    private String username;

    public User(String idToken, String email, String username) {
        IdToken = idToken;
        this.email = email;
        this.username = username;
    }

    public String getIdToken() {
        return IdToken;
    }

    public void setIdToken(String idToken) {
        IdToken = idToken;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}