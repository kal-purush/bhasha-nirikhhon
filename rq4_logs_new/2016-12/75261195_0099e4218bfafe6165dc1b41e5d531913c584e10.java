package com.example.hong.mylifelogger;

/**
 * Created by user on 2016-12-06.
 */

public class WalkData {
    private String dateString;
    private double count;

    WalkData(String date, double c){
        dateString = date;
        count = c;
    }
    public String getDateString(){
        return dateString;
    }
    public double getCount(){
        return count;
    }

}