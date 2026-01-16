package com.example.its_a_feature_not_a_bug;

import android.net.Uri;

public class Profile {
    private Uri profilePicId; // User profile pic
    // private String userId; // Unique ID for the user
    private String fullName; // Full name of the user
    private String contactInfo; // User contact info

    // Constructor
    public Profile(Uri profilePicId, String userId, String fullName, String contactInfo) {
        this.profilePicId = profilePicId;
        // this.userId = userId;
        this.fullName = fullName;
        this.contactInfo = contactInfo;
    }

    // Getters and Setters
    public Uri getProfilePicId() {
        return profilePicId;
    }

    public void setProfilePicId(Uri profilePicId) {
        this.profilePicId = profilePicId;
    }

//    public String getUserId() {
//        return userId;
//    }

//    public void setUserId(String userId) {
//        this.userId = userId;
//    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public String getContactInfo() {
        return contactInfo;
    }

    public void setContactInfo(String contactInfo) {
        this.contactInfo = contactInfo;
    }
}

