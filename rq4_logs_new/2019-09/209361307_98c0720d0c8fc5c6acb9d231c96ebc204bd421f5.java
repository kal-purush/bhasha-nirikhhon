package com.example.myapplication;

public class Controller {

    private final String profileId;
    private final String postId;

    public Controller(String profileId, String postId) {

        this.profileId = profileId;
        this.postId = postId;
    }

    public String getProfileId() {
        return profileId;
    }

    public String getPostId() {
        return postId;
    }
}