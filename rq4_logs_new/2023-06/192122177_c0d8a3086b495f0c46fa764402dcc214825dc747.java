package org.frank.java.jackson.beans;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Address {

    @JsonProperty("prov")
    private String province;

    @JsonProperty("cty")
    private String city;

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}