package com.quick.location.model;

import java.sql.Timestamp;
import java.util.Date;

import org.dozer.Mapping;

import com.google.firebase.database.Exclude;
import com.google.firebase.database.IgnoreExtraProperties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 
 * @author cfriasb
 *
 */
@Setter
@Getter
@ToString
@IgnoreExtraProperties
public class TopReviewFirebase {

    private String authorName;
    private Double rating;
    @Mapping("text")
    private String comment;
    @Mapping("place.placeId")
    private String placeId;
    private boolean done;
    @Mapping("place.name")
    private String name;
    private String date;
    
    
    

    @Exclude
    public String getPlaceId() {
        return placeId;
    }

    public void setPlaceId(String placeId) {
        this.placeId = placeId;
    }

}