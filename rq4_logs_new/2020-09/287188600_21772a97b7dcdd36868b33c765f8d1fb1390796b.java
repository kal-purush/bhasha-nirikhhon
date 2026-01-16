package com.example.woof.database;

import android.provider.BaseColumns;

public class productMaster {
    public productMaster() {
    }

    public static class product implements BaseColumns{
        public static final String TABLE_NAME = "Product";
        public static final String COLUMN_ID = "ProductID";
        public static final String COLUMN_NAME = "ProductName";
        public static final String COLUMN_DESC = "ProductDescription";
        public static final String COLUMN_PRICE = "ProductPrice";
        public  static final String COLUMN_SELLER = "SellerID";
    }
}