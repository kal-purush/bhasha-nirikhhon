package com.api.NomNomCounter.core.domain;

import java.util.UUID;

public class User {
    private UUID id;
    private String username;
    private String password;

    public User(){}

    public User(UUID id, String username, String password){
        this.id = id;
        this.username = username;
        this.password = password;
    }

    public UUID getUuid(){
        return id;
    }

    public String getUsername(){
        return username;
    }

    public void setUsername(String username){
        this.username = username;
    }

    public void setPassword(String password){
        this.password = password;
    }
}