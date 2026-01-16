package com.softdesign.school.data.storage.models;

import android.graphics.drawable.Drawable;

public class User {
    private String mFirstName;
    private String mLastName;
    private int mRate;
    private Drawable mImage;
    private String mVkLink;
    private String mGithubLink;
    private String mHometask;

    public User(String mFirstName, String mLastName, Drawable mImage) {
        this.mFirstName = mFirstName;
        this.mLastName = mLastName;
        this.mImage = mImage;
    }

    public String getmFirstName() {
        return mFirstName;
    }

    public String getmLastName() {
        return mLastName;
    }

    public int getmRate() {
        return mRate;
    }

    public Drawable getmImage() {
        return mImage;
    }

    public String getmVkLink() {
        return mVkLink;
    }

    public String getmGithubLink() {
        return mGithubLink;
    }

    public String getmHometask() {
        return mHometask;
    }
}