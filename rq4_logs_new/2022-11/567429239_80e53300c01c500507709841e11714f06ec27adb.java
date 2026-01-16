package com.mobilapp.geotagging;


import androidx.room.ColumnInfo;
import androidx.room.Entity;
import androidx.room.PrimaryKey;

@Entity
public class Tag {

    //used to make location objects in FusedLocationProvider
    @PrimaryKey
    public int tid;

    /*First declare Location object by doing
     Location myLocation = new Location(tagName);
     Locations are zeroed by default, so you need
     to set longitude and latitude.*/
    @ColumnInfo(name = "tag_name")
    public String tagName;

    /* set longitude with myLocation.setLongitude(logitude) */
    @ColumnInfo(name = "longitude")
    public double longitude;

    /* set latitude with myLocation.setLongitude(latitude) */
    @ColumnInfo(name = "latitude")
    public double latitude;
}