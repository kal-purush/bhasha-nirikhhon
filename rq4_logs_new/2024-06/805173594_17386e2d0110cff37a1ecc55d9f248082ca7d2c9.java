package com.example.BookingTravelBackend.payload.respone;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;
import java.util.Objects;

@Getter
@Setter
public class RevenueResponse {
    private int hotelId;
    private String address;
    private String Desbrice;
    private int totalPrice;
    private Date dateFrom;
    private Date dateTo;

    public RevenueResponse (Object[] revenua, Date dateFrom, Date dateTo){
        this.hotelId = Integer.parseInt(revenua[0].toString());
        this.address = revenua[1].toString();
        if (revenua[2] != null) {
            this.Desbrice = revenua[2].toString();
        }
        this.totalPrice = Integer.parseInt(revenua[3].toString());
        this.dateFrom = dateFrom;
        this.dateTo = dateTo;
    }
}