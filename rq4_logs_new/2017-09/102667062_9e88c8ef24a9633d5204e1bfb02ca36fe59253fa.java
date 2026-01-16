package com.foo.umbrella.database;

public class ConfigData {
    private String zipCode;
    private String unit;

    public ConfigData(){}

    public ConfigData(String zipCode, String unit){
        this.zipCode = zipCode;
        this.unit = unit;
    }

    public String getZipCode() {
        return zipCode;
    }

    public void setZipCode(String zipCode) {
        this.zipCode = zipCode;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }
}