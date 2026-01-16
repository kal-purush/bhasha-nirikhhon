package com.example.planties.data.plant.remote.dto;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class PlantReqPut {
    @SerializedName("name")
    public String name;

    @SerializedName("banner")
    public String banner;

    @SerializedName("imageBase64")
    public List<String> imageBase64;

    public PlantReqPut(String name, String banner, List<String> imageBase64) {
        this.name = name;
        this.banner = banner;
        this.imageBase64 = imageBase64;
    }

    public PlantReq mapToPlantReq(String dob, String type) {
        return new PlantReq(name, banner, dob, type, imageBase64);
    }
}