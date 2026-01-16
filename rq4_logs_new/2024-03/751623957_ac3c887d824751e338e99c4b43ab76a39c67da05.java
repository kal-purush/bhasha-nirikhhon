package com.example.its_a_feature_not_a_bug;

import java.io.Serializable;

public class Announcement implements Serializable {
    private String title;
    private String description;

    public Announcement(String title, String description) {
        this.title = title;
        this.description = description;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}