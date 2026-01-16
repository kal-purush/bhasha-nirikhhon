package org.launchcode.cheesemvc.models;

public enum CheeseType {
    HARD ("Hard"),
    SOFT ("Soft"),
    FAKE ("Fake");

    private final String description;

    CheeseType(String description) {
        this.description = description;
    }

    public String getDescription(){
        return this.description;
    }

}