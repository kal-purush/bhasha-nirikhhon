package com.example.its_a_feature_not_a_bug;

public class Event {
    private String name;
    private String date;
    private String location;
    private String description;

    public Event(String name, String date, String location, String description) {
        this.name = name;
        this.date = date;
        this.location = location;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDate() {
        return date;
    }

    public String getLocation() {
        return location;
    }

    public String getDescription() {
        return description;
    }
}