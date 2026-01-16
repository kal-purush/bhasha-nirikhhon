package com.modsen.passengerservice;

import com.modsen.passengerservice.dto.ListContainerResponseDto;
import com.modsen.passengerservice.dto.PassengerDto;
import com.modsen.passengerservice.model.Passenger;

import java.util.ArrayList;

public class AppTestUtil {

    public static final String PASSENGER_ENDPOINT = "/api/v1/passengers";
    public static final String PASSENGER_UPDATE_DELETE_ENDPOINT = "/api/v1/passengers/{passengerId}";

    public static final PassengerDto passengerRequestDto = new PassengerDto(
            null,
            "Jane",
            "Doe",
            "jane.doe@example.org",
            "6625550144",
            null,
            null);

    public static final PassengerDto invalidPassengerRequestDto = new PassengerDto(
            1L,
            "",
            "",
            "",
            "",
            5.0d,
            true);

    public static final PassengerDto passengerResponseDto = new PassengerDto(
            1L,
            "Jane",
            "Doe",
            "jane.doe@example.org",
            "6625550144",
            5d,
            false);

    public static final ListContainerResponseDto<PassengerDto> passengerPageResponseDto =
            new ListContainerResponseDto<>(1,
                    1,
                    1,
                    1L,
                    "Sort",
                    new ArrayList<>());


    public static final Passenger passenger;
    public static final PassengerDto passengerDto;

    static {
        passenger = new Passenger();
        passenger.setId(1L);
        passenger.setEmail("some.email@gmail.com");
        passenger.setFirstName("John");
        passenger.setLastName("Lock");
        passenger.setAverageRating(5.0);
        passenger.setPhone("+123456789");
        passenger.setDeleted(false);
        passengerDto = new PassengerDto(
                passenger.getId(),
                passenger.getFirstName(),
                passenger.getLastName(),
                passenger.getEmail(),
                passenger.getPhone(),
                passenger.getAverageRating(),
                passenger.getDeleted());
    }

}