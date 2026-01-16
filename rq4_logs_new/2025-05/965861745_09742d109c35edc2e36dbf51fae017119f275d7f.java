package com.daclink.citypulse.model;

import androidx.room.Entity;
import androidx.room.PrimaryKey;

@Entity(tableName = "cached_events")
public class CachedEvent {

    @PrimaryKey(autoGenerate = true)
    private int id;

    private String city;
    private String category;
    private String title;
    private String venue;
    private String date;

    public CachedEvent(String city, String category, String title, String venue, String date) {
        this.city = city;
        this.category = category;
        this.title = title;
        this.venue = venue;
        this.date = date;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCity() {
        return city;
    }

    public String getCategory() {
        return category;
    }

    public String getTitle() {
        return title;
    }

    public String getVenue() {
        return venue;
    }

    public String getDate() {
        return date;
    }
}