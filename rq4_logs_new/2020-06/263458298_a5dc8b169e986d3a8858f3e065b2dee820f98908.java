package com.xml.dto;

import com.xml.enummeration.RentRequestStatus;

import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;
import java.util.Set;

public class RentRequestDto {

    private Long id;

    @NotNull(message = "Date reserved from cannot be empty")
    private LocalDateTime reservedFrom;

    @NotNull(message = "Date reserved to cannot be empty")
    private LocalDateTime reservedTo;

    @NotNull
    private Set<AdvertisementDto> advertisementsForRent;

    private RentRequestStatus rentRequestStatus;


    public RentRequestDto() {
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public LocalDateTime getReservedFrom() {
        return reservedFrom;
    }

    public void setReservedFrom(LocalDateTime reservedFrom) {
        this.reservedFrom = reservedFrom;
    }

    public LocalDateTime getReservedTo() {
        return reservedTo;
    }

    public void setReservedTo(LocalDateTime reservedTo) {
        this.reservedTo = reservedTo;
    }

    public Set<AdvertisementDto> getAdvertisementsForRent() {
        return advertisementsForRent;
    }

    public void setAdvertisementsForRent(Set<AdvertisementDto> advertisementsForRent) {
        this.advertisementsForRent = advertisementsForRent;
    }

    public RentRequestStatus getRentRequestStatus() {
        return rentRequestStatus;
    }

    public void setRentRequestStatus(RentRequestStatus rentRequestStatus) {
        this.rentRequestStatus = rentRequestStatus;
    }

    @Override
    public String toString() {
        return "RentRequestDto{" +
                "id=" + id +
                ", reservedFrom=" + reservedFrom +
                ", reservedTo=" + reservedTo +
                ", advertisementsForRent=" + advertisementsForRent +
                ", rentRequestStatus=" + rentRequestStatus +
                '}';
    }
}