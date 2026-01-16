package com.example.appchiasecongthucnauan.models;

import com.google.gson.annotations.SerializedName;

public class BookmarkDto {
    @SerializedName("id")
    private String id;
    
    @SerializedName("recipeId") 
    private String recipeId;
    
    @SerializedName("title")
    private String title;
    
    @SerializedName("creatorName")
    private String creatorName;
    
    @SerializedName("mediaUrl")
    private String mediaUrl;
    
    @SerializedName("createdAt")
    private String createdAt;

    // Getters
    public String getId() { return id; }
    public String getRecipeId() { return recipeId; }
    public String getTitle() { return title; }
    public String getCreatorName() { return creatorName; }
    public String getMediaUrl() { return mediaUrl; }
    public String getCreatedAt() { return createdAt; }
}