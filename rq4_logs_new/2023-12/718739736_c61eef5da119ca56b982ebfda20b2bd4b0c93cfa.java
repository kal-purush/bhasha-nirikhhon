package com.example.planties.data.auth.remote.dto;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import javax.annotation.Nullable;

public class RegisterRequest {
    @Expose
    @SerializedName("username")
    private String username;

    @Expose
    @SerializedName("name")
    private String name;

    @SerializedName("password")
    private String password;


    @SerializedName("profile_image")
    @Nullable
    private String profile_image;

    public void setName(String name) {
        this.name = name;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setProfile_image(String image) {
        this.profile_image = image;
    }
}