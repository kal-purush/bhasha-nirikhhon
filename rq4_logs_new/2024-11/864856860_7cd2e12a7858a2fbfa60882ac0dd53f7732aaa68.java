package com.modsen.ridesservice;

import com.modsen.ridesservice.client.driver.DriverResponseDto;
import com.modsen.ridesservice.client.passenger.PassengerResponseDto;
import com.modsen.ridesservice.dto.ListContainerResponseDto;
import com.modsen.ridesservice.dto.request.RideRequestDto;
import com.modsen.ridesservice.dto.request.RideStatusRequestDto;
import com.modsen.ridesservice.dto.response.RideResponseDto;

import java.util.List;

public class AppIntegrationTestUtil {

    public static final long PASSENGER_ID = 1L;
    public static final long DRIVER_ID = 1L;
    public static final long RIDE_ID = 1L;

    public static final String IGNORING_FIELD_ONE = "orderDateTime";
    public static final String IGNORING_FIELD_TWO = "cost";
    public static final String INNER_IGNORING_FIELD_ONE = "values.orderDateTime";
    public static final String INNER_IGNORING_FIELD_TWO = "values.cost";

    public static final String PASSENGER_FROM_ANOTHER_SERVICE_ENDPOINT = "/api/v1/passengers/" + PASSENGER_ID;
    public static final String DRIVER_FROM_ANOTHER_SERVICE_ENDPOINT = "/api/v1/drivers/" + DRIVER_ID;

    private static final long ID_AFTER_CREATE = 2L;
    private static final long ID = 1L;

    private static final String RIDE_STATUS_ONE = "CREATED";
    private static final String RIDE_STATUS_TWO = "ACCEPTED";

    private static final String SORT_TYPE = "UNSORTED";
    private static final int TOTAL_PAGES = 1;
    private static final int CURRENT_LIMIT = 10;
    private static final int CURRENT_OFFSET = 0;
    private static final int TOTAL_ELEMENTS = 1;

    private static final String DEPARTURE_ADDRESS_FOR_CREATE = "Vilnius";
    private static final String DESTINATION_ADDRESS_FOR_CREATE = "Riga";

    private static final String DEPARTURE_ADDRESS_FOR_UPDATE = "Moscow";
    private static final String DESTINATION_ADDRESS_FOR_UPDATE = "Minsk";

    private static final String DEPARTURE_ADDRESS_FOR_GET_OR_CHANGE_STATUS = "123 Main St";
    private static final String DESTINATION_ADDRESS_FOR_GET_OR_CHANGE_STATUS = "456 Elm St";

    private static final String GENDER = "Male";
    private static final String FIRST_NAME = "John";
    private static final String LAST_NAME = "Lock";
    private static final String PHONE = "+1234567890";
    private static final String NAME = "John Lock";
    private static final String EMAIL = "john.lock@gmail.com";


    public static final String SQL_DELETE_ALL_DATA = """
            DELETE FROM ride;
            """;

    public static final String SQL_RESTART_SEQUENCES = """
            ALTER SEQUENCE ride_id_seq RESTART WITH 1;
            """;

    public static final String SQL_INSERT_DATA = """
            INSERT INTO ride (driver_id, passenger_id, departure_address, destination_address, ride_status, order_date_time, cost)
            VALUES (1, 1, '123 Main St', '456 Elm St', 'CREATED', '2023-10-01 10:00:00', 12.50);
            """;

    public static final RideRequestDto RIDE_REQUEST_CREATE_DTO = new RideRequestDto(
            DRIVER_ID,
            PASSENGER_ID,
            DEPARTURE_ADDRESS_FOR_CREATE,
            DESTINATION_ADDRESS_FOR_CREATE);

    public static final RideResponseDto RIDE_RESPONSE_CREATE_DTO = new RideResponseDto(
            ID_AFTER_CREATE,
            DRIVER_ID,
            PASSENGER_ID,
            DEPARTURE_ADDRESS_FOR_CREATE,
            DESTINATION_ADDRESS_FOR_CREATE,
            RIDE_STATUS_ONE,
            null,
            null);

    public static final RideRequestDto RIDE_REQUEST_UPDATE_DTO = new RideRequestDto(
            null,
            null,
            DEPARTURE_ADDRESS_FOR_UPDATE,
            DESTINATION_ADDRESS_FOR_UPDATE);

    public static final RideResponseDto RIDE_RESPONSE_UPDATE_DTO = new RideResponseDto(
            ID,
            DRIVER_ID,
            PASSENGER_ID,
            DEPARTURE_ADDRESS_FOR_UPDATE,
            DESTINATION_ADDRESS_FOR_UPDATE,
            RIDE_STATUS_ONE,
            null,
            null);

    public static final RideStatusRequestDto RIDE_STATUS_CHANGE_REQUEST_DTO = new RideStatusRequestDto(RIDE_STATUS_TWO);

    public static final RideResponseDto RIDE_RESPONSE_AFTER_CHANGE_STATUS_DTO = new RideResponseDto(
            ID,
            DRIVER_ID,
            PASSENGER_ID,
            DEPARTURE_ADDRESS_FOR_GET_OR_CHANGE_STATUS,
            DESTINATION_ADDRESS_FOR_GET_OR_CHANGE_STATUS,
            RIDE_STATUS_TWO,
            null,
            null);

    public static final RideResponseDto RIDE_RESPONSE_GET_DTO = new RideResponseDto(
            ID,
            DRIVER_ID,
            PASSENGER_ID,
            DEPARTURE_ADDRESS_FOR_GET_OR_CHANGE_STATUS,
            DESTINATION_ADDRESS_FOR_GET_OR_CHANGE_STATUS,
            RIDE_STATUS_ONE,
            null,
            null);

    public static final PassengerResponseDto PASSENGER_RESPONSE_DTO = new PassengerResponseDto(
            ID,
            FIRST_NAME,
            LAST_NAME,
            EMAIL,
            PHONE,
            false);

    public static final DriverResponseDto DRIVER_RESPONSE_DTO = new DriverResponseDto(
            ID,
            NAME,
            EMAIL,
            PHONE,
            GENDER,
            false);

    public static final ListContainerResponseDto<RideResponseDto> PAGE_RIDE_RESPONSE_DTO =
            ListContainerResponseDto.<RideResponseDto>builder()
                    .withSort(SORT_TYPE)
                    .withTotalPages(TOTAL_PAGES)
                    .withCurrentLimit(CURRENT_LIMIT)
                    .withCurrentOffset(CURRENT_OFFSET)
                    .withTotalElements(TOTAL_ELEMENTS)
                    .withValues(List.of(RIDE_RESPONSE_GET_DTO))
                    .build();

}