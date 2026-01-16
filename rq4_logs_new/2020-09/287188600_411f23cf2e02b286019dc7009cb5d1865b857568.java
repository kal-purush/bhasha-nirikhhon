package com.example.woof.database;

public class storyModel {
    private int storyId;
    private String Title;
    private String Descrip;
    private String userId;


    public storyModel(int storyId, String title, String descrip, String userId) {
        this.storyId = storyId;
        Title = title;
        Descrip = descrip;
        this.userId = userId;
    }

    public int getStoryId() {
        return storyId;
    }

    public void setStoryId(int storyId) {
        this.storyId = storyId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "storyModel{" +
                "Title='" + Title + '\'' +
                ", Descrip='" + Descrip + '\'' +
                '}';
    }
    public String getTitle() {
        return Title;
    }

    public String getDescrip() {
        return Descrip;
    }

    public storyModel() {
    }

    public void setTitle(String title) {
        Title = title;
    }

    public void setDescrip(String descrip) {
        Descrip = descrip;
    }
}