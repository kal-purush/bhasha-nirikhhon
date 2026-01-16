package com.example.justpoteito.models;

public class UserImage {
    private int id;
    private String image;
    private String message;

    public UserImage() {
    }

    public UserImage(int id, String image, String message) {
        this.id = id;
        this.image = image;
        this.message = message;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "UserImage{" +
                "id=" + id +
                ", image='" + image + '\'' +
                ", image='" + message + '\'' +
                '}';
    }
}