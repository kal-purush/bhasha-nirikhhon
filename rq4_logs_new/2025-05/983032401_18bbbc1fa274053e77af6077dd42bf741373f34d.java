package com.api.NomNomCounter.application.core.domain.user;

import java.util.UUID;

public class User {
    private Integer idUser;
    private String username;
    private String passwordUser;

    public User(){}
    public User(String username, String passwordUser){
        this.username = username;
        this.passwordUser = passwordUser;
    }

    public Integer getIdUser() {
        return idUser;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPasswordUser(){
        return passwordUser;
    }
    public void setPasswordUser(String passwordUser) {
        this.passwordUser = passwordUser;
    }
    
}