package com.apiautomation.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ResponseObject {
    /*
     * {
     * "id": "7",
     * "name": "Apple MacBook Pro 16",
     * "data": [{
     * "year": 2019,
     * "price": 1849.99,
     * "CPU model": "Intel Core i9",
     * "Hard disk size": "1 TB"
     * }],
     * "createdAt": "2022-11-21T20:06:23.986Z"
     * }
     */

    @JsonProperty("id")
    public String id;

    @JsonProperty("name")
    public String name;

    @JsonProperty("data")
    public DataItem dataItem;

    @JsonProperty("createdAt")
    public String createdAt;

    public class DataItem {
        @JsonProperty("year")
        public int year;

        @JsonProperty("price")
        public int price;

        @JsonProperty("CPU model")
        public String cpuModel;

        @JsonProperty("Hard disk size")
        public String hardDiskSize;
    }
}