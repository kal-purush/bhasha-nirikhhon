package com.example.carwash.utils;

import com.ittianyu.bottomnavigationviewex.BottomNavigationViewEx;

//object class to make it easier to setup the bottom navbar for all the separate profile views
public class BottomNavigationHelper {

    public static void setUpBottomNavigationView(BottomNavigationViewEx bottomNavigationView){
        bottomNavigationView.enableAnimation(false);
        bottomNavigationView.enableShiftingMode(false);
        bottomNavigationView.enableItemShiftingMode(false);
        bottomNavigationView.setTextVisibility(false);
    }

}