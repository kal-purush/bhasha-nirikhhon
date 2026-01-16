package com.modsen.ridesservice;

import com.modsen.ridesservice.dto.ListContainerResponseDto;
import com.modsen.ridesservice.dto.request.RideRequestDto;
import com.modsen.ridesservice.dto.request.RideStatusRequestDto;
import com.modsen.ridesservice.dto.response.RideResponseDto;
import com.modsen.ridesservice.model.Ride;
import com.modsen.ridesservice.model.enums.RideStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;

public class AppTestUtil {

    public static final String RIDE_PAGE_GET_POST_ENDPOINT = "/api/v1/rides";
    public static final String RIDE_GET_ENDPOINT = "/api/v1/rides/1";
    public static final String RIDE_BY_DRIVER_ID_GET_ENDPOINT = "/api/v1/rides/drivers/{driverId}";
    public static final String RIDE_BY_PASSENGER_ID_GET_ENDPOINT = "/api/v1/rides/passengers/{passengerId}";
    public static final String RIDE_UPDATE_ENDPOINT = "/api/v1/rides/{rideId}";
    public static final String RIDE_CHANGE_RIDE_STATUS_ENDPOINT = "/api/v1/rides/{rideId}/status";


    public static final RideRequestDto rideRequestDto = new RideRequestDto(
            1L,
            1L,
            "Vilnius",
            "Riga");
    public static final RideResponseDto rideResponseDto = new RideResponseDto(
            1L,
            1L,
            1L,
            "Vilnius",
            "Riga",
            "CREATED",
            LocalDateTime.parse("2023-10-31T14:30:00"),
            new BigDecimal(231));

    public static final ListContainerResponseDto<RideResponseDto> ridePageResponseDto = new ListContainerResponseDto<>(
            1, 1, 1, 1L,
            "Sort", new ArrayList<>());


    public static final RideRequestDto rideRequestInServiceDto;
    public static final RideResponseDto rideResponseInServiceDto;
    public static final RideStatusRequestDto rideStatusRequestDto;
    public static final Ride ride;

    static {
        rideRequestInServiceDto = new RideRequestDto(
                1L,
                1L,
                "123 Main St",
                "456 Elm St"
        );

        rideStatusRequestDto = new RideStatusRequestDto(
                RideStatus.CREATED.name()
        );

        rideResponseInServiceDto = new RideResponseDto(
                1L,
                1L,
                1L,
                "123 Main St",
                "456 Elm St",
                RideStatus.CREATED.name(),
                LocalDateTime.parse("2023-10-31T14:30:00"),
                new BigDecimal("100.00")
        );

        ride = new Ride();
        ride.setId(1L);
        ride.setDriverId(1L);
        ride.setPassengerId(1L);
        ride.setDepartureAddress("123 Main St");
        ride.setDestinationAddress("456 Elm St");
        ride.setRideStatus(RideStatus.CREATED);
        ride.setOrderDateTime(LocalDateTime.parse("2023-10-31T14:30:00"));
        ride.setCost(new BigDecimal("100.00"));
    }

}