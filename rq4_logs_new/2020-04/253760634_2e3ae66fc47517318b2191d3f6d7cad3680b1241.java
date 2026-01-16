package com.example.airbnbapi.model;

import com.fasterxml.jackson.annotation.JsonProperty;


public abstract class Media {

   @JsonProperty("id")
    private  int id;
    @JsonProperty("title")
    private  String title;
    @JsonProperty("year")
    private  long year;
    @JsonProperty("creator")
    private  String creator;




    public int getId() {
        return id;
    }


    public String getTitle() {
        return title;
    }



    public long getYear() {
        return year;
    }


    public String getCreator() {
        return creator;
    }


    @Override
    public String toString() {
        return
                "id='" + id + '\'' +
                        ", title='" + title + '\'' +
                        ", year=" + year +
                        ", authors=" + creator +
                        '}';
    }
}