package com.bassett.health_tracker_app;

import java.util.Date;

import androidx.room.ColumnInfo;
import androidx.room.Entity;
import androidx.room.PrimaryKey;
import androidx.room.TypeConverters;


@Entity
public class Exercise {

    @PrimaryKey(autoGenerate = true)
    public long id;

    public String title;
    public String quantity;
    public String description;
    public String timestamp;

    //typeconverter for date
//    @ColumnInfo(name = "created_at")
//    @TypeConverters({TimestampConverter.class})
//    public Date timestamp;



    //constructor
    public Exercise (String title, String quantity, String description, String timestamp){

        this.title=title;
        this.quantity=quantity;
        this.description=description;
        this.timestamp=timestamp;

    }

    public String toString() {
        return this.title + ": " + this.description + " - " + this.timestamp;
    }
}