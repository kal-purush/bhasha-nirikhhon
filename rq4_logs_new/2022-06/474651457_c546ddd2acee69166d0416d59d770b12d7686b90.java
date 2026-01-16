package com.example.projectmrsisa.dto;

public class ServiceQueryDTO {
    private String name;
    private String address;
    private String city;
    private String country;
    private String promotionalDescription;
    private String serviceType;

    public ServiceQueryDTO() {
    }

    public ServiceQueryDTO(String name, String address, String city, String country, String promotionalDescription, String serviceType) {
        this.name = name;
        this.address = address;
        this.city = city;
        this.country = country;
        this.promotionalDescription = promotionalDescription;
        this.serviceType = serviceType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getPromotionalDescription() {
        return promotionalDescription;
    }

    public void setPromotionalDescription(String promotionalDescription) {
        this.promotionalDescription = promotionalDescription;
    }

    public String getServiceType() {
        return serviceType;
    }

    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
    }
}