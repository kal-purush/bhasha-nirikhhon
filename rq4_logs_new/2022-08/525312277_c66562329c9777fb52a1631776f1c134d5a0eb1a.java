package com.example.gonggi.dto;

import lombok.*;

@Data
public class VisitDto {
    String place;
    long cnt;

    public VisitDto(String place, long cnt){
        this.place = place;
        this.cnt = cnt;
    }


}