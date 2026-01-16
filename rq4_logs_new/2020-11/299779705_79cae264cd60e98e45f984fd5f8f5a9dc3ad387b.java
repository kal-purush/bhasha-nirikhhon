package com.example.cmput301f20t18;

public class userNotification {
    private String id;
    private String message;

    public userNotification(String id, String message) {
        this.id = id;
        this.message = message;
    }

    public userNotification() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}