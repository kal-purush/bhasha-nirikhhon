package com.example.eventz;

import android.content.Intent;

public class Event {
    private String name;
    private int imageUrl;
    public Event() {

    }
    public Event(String name, Integer imageUrl) {
        this.name = name;
        this.imageUrl = imageUrl;
    }

    public Integer getImageUrl() {
        return imageUrl;
    }

    public void setImageUrl(Integer imageUrl) {
        this.imageUrl = imageUrl;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}