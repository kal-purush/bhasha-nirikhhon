package com.example.woof.database;

public class reviewsModel {
    private int reviewID;
    private String reviewDescription;
    private int reviewRating;
    private int accessoriesID;
    private int userID;

    public reviewsModel(int reviewID, String reviewDescription, int reviewRating, int accessoriesID, int userID) {
        this.reviewID = reviewID;
        this.reviewDescription = reviewDescription;
        this.reviewRating = reviewRating;
        this.accessoriesID = accessoriesID;
        this.userID = userID;
    }

    public int getReviewID() {
        return reviewID;
    }

    public void setReviewID(int reviewID) {
        this.reviewID = reviewID;
    }

    public String getReviewDescription() {
        return reviewDescription;
    }

    public void setReviewDescription(String reviewDescription) {
        this.reviewDescription = reviewDescription;
    }

    public int getReviewRating() {
        return reviewRating;
    }

    public void setReviewRating(int reviewRating) {
        this.reviewRating = reviewRating;
    }

    public int getAccessoriesID() {
        return accessoriesID;
    }

    public void setAccessoriesID(int accessoriesID) {
        this.accessoriesID = accessoriesID;
    }

    public int getUserID() {
        return userID;
    }

    public void setUserID(int userID) {
        this.userID = userID;
    }
}
