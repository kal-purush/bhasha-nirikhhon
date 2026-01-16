package com.apiautomation.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class updateObjectResponse {

    @JsonProperty("id")
    public String id;

    @JsonProperty("name")
    public String name;

    @JsonProperty("data")
    public DataItem dataItem;

    @JsonProperty("createdAt")
    public String createdAt;

    @JsonProperty("updatedAt")
    public String updatedAt;

    public class DataItem {
        @JsonProperty("year")
        public int year;

        @JsonProperty("price")
        public float price;

        @JsonProperty("CPU model")
        public String cpuModel;

        @JsonProperty("Hard disk size")
        public String hardDiskSize;

        @JsonProperty("color")
        public String color;
    }
}