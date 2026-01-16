package com.ebot.ebotgo;

public class User {
    private final String username;
    private final String userId;
    private final int permission;

    public User(String userId,String username,int permission){
        this.permission=permission;
        this.userId=userId;
        this.username=username;
    }

    public int getPermission() {
        return permission;
    }

    public String getUserId() {
        return userId;
    }

    public String getUsername() {
        return username;
    }
}