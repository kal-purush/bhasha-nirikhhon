package com.example.woof.database;

import android.provider.BaseColumns;

public class petOwnerMaster {
    public petOwnerMaster() {
    }
    public static class petOwner implements BaseColumns{
        public static final String TABLE_NAME = "PetOwner";
        public static final String COLUMN_ID = "PetOwnerID";
        public static final String COLUMN_FNAME = "FirstName";
        public static final String COLUMN_LNAME = "LastName";
        public static final String COLUMN_CONTACT = "ContactNumber";
        public static final String COLUMN_DOB = "DateOfBirth";
        public static final String COLUMN_EMAIL = "Email";
        public static final String COLUMN_PWD = "Password";


    }
}