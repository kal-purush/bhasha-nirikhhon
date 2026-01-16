package org.tndata.officehours.model;

import com.google.gson.annotations.SerializedName;


/**
 * Model for the new representation of office hours.
 *
 * @author Ismael Alonso
 */
public class OfficeHours{
    @SerializedName("Monday")
    private String monday[][];
    @SerializedName("Tuesday")
    private String tuesday[][];
    @SerializedName("Wednesday")
    private String wednesday[][];
    @SerializedName("Thursday")
    private String thursday[][];
    @SerializedName("Friday")
    private String friday[][];
    @SerializedName("Saturday")
    private String saturday[][];
    @SerializedName("Sunday")
    private String sunday[][];


    public boolean hasMondayHours(){
        return monday != null;
    }

    public boolean hasTuesdayHours(){
        return monday != null;
    }

    public boolean hasWednesdayHours(){
        return monday != null;
    }

    public boolean hasThursdayHours(){
        return monday != null;
    }

    public boolean hasFridayHours(){
        return monday != null;
    }

    public boolean hasSaturdayHours(){
        return monday != null;
    }

    public boolean hasSundayHours(){
        return monday != null;
    }

    private String getHoursFor(String[][] day){
        String result = "";
        for (int i = 0; i < day.length; i++){
            result += day[i][0] + "-" + day[i][1];
            if (day.length < 2){
                result += ", ";
                if (i == day.length-2){
                    result += "and ";
                }
            }
            else if (day.length == 2 && i == 0){
                result += " and ";
            }
        }
        return result;
    }

    public String getMondayHours(){
        if (hasMondayHours()){
            return getHoursFor(monday);
        }
        return "";
    }

    public String getTuesdayHours(){
        if (hasTuesdayHours()){
            return getHoursFor(tuesday);
        }
        return "";
    }

    public String getWednesdayHours(){
        if (hasWednesdayHours()){
            return getHoursFor(wednesday);
        }
        return "";
    }

    public String getThursdayHours(){
        if (hasThursdayHours()){
            return getHoursFor(thursday);
        }
        return "";
    }

    public String getFridayHours(){
        if (hasFridayHours()){
            return getHoursFor(friday);
        }
        return "";
    }

    public String getSaturdayHours(){
        if (hasSaturdayHours()){
            return getHoursFor(saturday);
        }
        return "";
    }

    public String getSundayHours(){
        if (hasSundayHours()){
            return getHoursFor(sunday);
        }
        return "";
    }
}