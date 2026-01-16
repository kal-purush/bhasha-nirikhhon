package com.xml.dto;

import java.time.LocalDateTime;
import java.util.Set;

public class AdvertisementDto {

    private Long id;
    private CarDto car;
    private UserDto advertiser;
    private LocalDateTime availableFrom;
    private LocalDateTime availableTo;
    private Set<CommentDto> comments;
    private PricelistDto pricelist;

    public AdvertisementDto() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public CarDto getCar() {
        return car;
    }

    public void setCar(CarDto car) {
        this.car = car;
    }

    public UserDto getAdvertiser() {
        return advertiser;
    }

    public void setAdvertiser(UserDto advertiser) {
        this.advertiser = advertiser;
    }

    public LocalDateTime getAvailableFrom() {
        return availableFrom;
    }

    public void setAvailableFrom(LocalDateTime availableFrom) {
        this.availableFrom = availableFrom;
    }

    public LocalDateTime getAvailableTo() {
        return availableTo;
    }

    public void setAvailableTo(LocalDateTime availableTo) {
        this.availableTo = availableTo;
    }

    public Set<CommentDto> getComments() {
        return comments;
    }

    public void setComments(Set<CommentDto> comments) {
        this.comments = comments;
    }

    public PricelistDto getPricelist() {
        return pricelist;
    }

    public void setPricelist(PricelistDto pricelist) {
        this.pricelist = pricelist;
    }
}