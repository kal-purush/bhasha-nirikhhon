package com.dtavana.foodswipe.models;

import com.parse.ParseClassName;
import com.parse.ParseObject;

@ParseClassName("Restaurant")
public class Restaurant extends ParseObject {

    public static final String KEY_VISITEDCOUNT = "visitedCount";
    public static final String KEY_NAME = "name";
    public static final String KEY_RESTAURANTID = "restaurantId";

    public Number getVisitedCount() { return getNumber(KEY_VISITEDCOUNT); }
    public void setVisitedCount(Number visitedCount) { put(KEY_VISITEDCOUNT, visitedCount); }

    public String getName() { return getString(KEY_NAME); }
    public void setName(String name) { put(KEY_NAME, name); }

    public Number getRestaurantId() { return getNumber(KEY_RESTAURANTID); }
    public void setRestaurantId(Number restaurantId) { put(KEY_RESTAURANTID, restaurantId); }
}