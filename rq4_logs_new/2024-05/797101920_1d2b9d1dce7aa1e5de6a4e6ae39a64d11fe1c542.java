package com.example.EnjoyTripBackend.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Golf extends BaseTimeDomain{

    private Long id;
    private String place;
    private String location;
    private String date;
    private String golfImageUrl;
    private int greenFeeWeek;
    private int greenFeeWeekend;
    private double latitude;
    private double longitude;

}