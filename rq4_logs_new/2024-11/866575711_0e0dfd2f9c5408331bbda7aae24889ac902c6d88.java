package com.example.appchiasecongthucnauan.models.explore;

public class RecentRecipeDto {
    private String id;
    private String title;
    private String chefName;
    private String imageUrl;
    private String createdAt;

    // Getters
    public String getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public String getChefName() {
        return chefName;
    }

    public String getImageUrl() {
        return imageUrl;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    // Setters
    public void setId(String id) {
        this.id = id;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setChefName(String chefName) {
        this.chefName = chefName;
    }

    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }
}