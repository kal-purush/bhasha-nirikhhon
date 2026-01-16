package com.api.NomNomCounter.domain.model;
import java.util.UUID;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

@Entity
public class User {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private UUID id;
    private String username;
    private String password;

    public User(){

    }

    public User(UUID id, String username, String password) {
        this.id = id;
        this.username = username;
        this.password = password;
    }

    public UUID getId(){
        return id;
    }

    public String getUsername(){
        return username;
    }

    public String setUsername(String username){
        this.username = username;
        return username;
    }

    public String getPassword(){
        return password;
    }

    public String setPassword(String password){
        this.password = password;
        return password;
    }

}