package com.novi.eindopdracht.recipeDiary.recipeDiary.dto;

import java.util.List;

public class UserCredentialsDto {

    public String username;

    public List<String> roles;

    public UserCredentialsDto(String username, List<String> roles) {
        this.username = username;
        this.roles = roles;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public List<String> getRoles() {
        return roles;
    }

    public void setRoles(List<String> roles) {
        this.roles = roles;
    }
}