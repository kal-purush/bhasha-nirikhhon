package com.example.EnjoyTripBackend.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Camping extends BaseTimeDomain{

    private Long id;
    private String name;
    private String shortIntro;
    private String Intro;
    private String zipcode;
    private String address;
    private double latitude;
    private double longitude;
    private String imageUrl;
    private String homepage;
    private String tel;
}