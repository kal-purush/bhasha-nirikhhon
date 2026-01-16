package com.example.trekkin.ui.explore;

import android.net.Uri;

public class LocationCard {
    private String mName;
    private String mImageUrl;

    public LocationCard() {
        //Empty constructor needed for firebase connectivity.
    }

    public LocationCard(String name, Uri imageUrl) {
        if (name.trim().equals("")) {
            name = "No Name";
        }
        mName = name;
        mImageUrl = imageUrl.toString();
    }

    public String getmName() {
        return mName;
    }

    public void setmName(String mName) {
        this.mName = mName;
    }

    public String getmImageUrl() {
        return mImageUrl;
    }

    public void setmImageUrl(String mImageUrl) {
        this.mImageUrl = mImageUrl;
    }
}