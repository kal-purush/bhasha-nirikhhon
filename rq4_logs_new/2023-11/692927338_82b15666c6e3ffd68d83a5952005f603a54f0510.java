package com.example.hihotel.domain;

import lombok.Getter;

import javax.persistence.*;

@Entity
@Getter
public class Booking {

    @EmbeddedId
    private BookingId bookingId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "cid", insertable = false, updatable = false)
    private Customer customer;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "hid", referencedColumnName = "hid", insertable = false, updatable = false)
    @JoinColumn(name = "roomnum", referencedColumnName = "roomnum", insertable = false, updatable = false)
    private HotelRoom hotelRoom;

    private String checkInDate;

    private String checkOutDate;
}

