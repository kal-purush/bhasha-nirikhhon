package com.example.woof.database;

import android.provider.BaseColumns;

public class sellerMaster {
    public sellerMaster() {
    }
    public static class seller implements BaseColumns {
        public static final String TABLE_NAME = "Seller";
        public static final String COLUMN_ID = "SellerID";
        public static final String COLUMN_NAME = "Name";
        public static final String COLUMN_EMAIL = "Email";
        public static final String COLUMN_CONTACT = "ContactNo";
        public static final String COLUMN_ADDRESS = "Address";
        public static final String COLUMN_PWD = "Password";
    }

}