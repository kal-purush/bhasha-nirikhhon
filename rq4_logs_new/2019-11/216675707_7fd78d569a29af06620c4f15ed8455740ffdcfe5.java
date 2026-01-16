/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lp2.telegrammanager.models;

/**
 *
 * @author mathe
 */
public class Place {
    public String name;
    public String description;
    public int id;
    
    public Place(String name, String description){
        this.name = name;
        this.description = description;
    }
    
    public Place(int id, String name, String description){
        this.id = id;
        this.name = name;
        this.description = description;
    }
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Place{" + "name=" + name + ", description=" + description + ", id=" + id + '}';
    }
    
    
}