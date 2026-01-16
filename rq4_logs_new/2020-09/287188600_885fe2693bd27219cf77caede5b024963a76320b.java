package com.example.woof.database;

import android.provider.BaseColumns;

public class reviewsMaster {
    public reviewsMaster(){

    }
    public static class myReviews implements BaseColumns{
        public static final String TABLE_NAME = "myReviews";
        public static final String COLUMN_ID = "reviewID";
        public static final String COLUMN_DESC = "reviewDescription";
        public static final String COLUMN_RATING = "reviewRating";
        public static final String COLUMN_ACCESSORY_ID = "accessoryId";
        public static final String COLUMN_USER_ID = "userId";
    }
}