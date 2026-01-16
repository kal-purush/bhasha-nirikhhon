package com.example.woof.database;

import android.provider.BaseColumns;

public class accessoriesMaster {

    public accessoriesMaster(){

    }

    public static class Accessories implements BaseColumns{
        public static final String TABLE_NAME = "Accessories";
        public static final String COLUMN_ID = "Accessories ID";
        public static final String COLUMN_NAME = "Name";
        public static final String COLUMN_DESC = "Description";
        public static final String COLUMN_ = "Images";
        public static final String COLUMN_PRICE = "Price";
        public static final String COLUMN_SELLER_ID = "Seller";

    }
}