package com.toyhe.app.Auth.Dtos.Requests;

public record AddressRequest(
        String province,
        String town,
        String commune,
        String quarter,
        String avenue ,
        long number
) {

}