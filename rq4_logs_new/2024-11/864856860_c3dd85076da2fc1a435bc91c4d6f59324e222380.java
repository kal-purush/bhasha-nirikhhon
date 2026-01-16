package com.modsen.passengerservice;

import com.modsen.passengerservice.dto.ListContainerResponseDto;
import com.modsen.passengerservice.dto.PassengerDto;

import java.util.List;

public class IntegrationTestData {

    public static final long PASSENGER_ID = 1L;

    private static final long ID = 1L;
    private static final long ID_AFTER_CREATE = 2L;

    private static final String PASSENGER_FIRST_NAME_FOR_CREATE = "John";
    private static final String PASSENGER_LAST_NAME_FOR_CREATE = "Lock";
    private static final String PASSENGER_EMAIL_FOR_CREATE = "john.lock@gmail.com";
    private static final String PASSENGER_PHONE_FOR_CREATE_UPDATE = "+1234567890";

    private static final String PASSENGER_FIRST_NAME_FOR_GET_UPDATE = "Ivan";
    private static final String PASSENGER_LAST_NAME_FOR_GET_UPDATE = "Ivanov";
    private static final String PASSENGER_EMAIL_FOR_GET_UPDATE = "ivan1@example.com";
    private static final String PASSENGER_PHONE_FOR_GET = "+375336402547";

    private static final String SORT_TYPE = "UNSORTED";
    private static final int TOTAL_PAGES = 1;
    private static final int CURRENT_LIMIT = 10;
    private static final int CURRENT_OFFSET = 0;
    private static final int TOTAL_ELEMENTS = 1;

    public static final String SQL_DELETE_ALL_DATA = """
            DELETE FROM passenger;
            """;

    public static final String SQL_RESTART_SEQUENCES = """
            ALTER SEQUENCE passenger_id_seq RESTART WITH 1;
            """;

    public static final String SQL_INSERT_DATA = """
            INSERT INTO passenger (first_name, last_name, email, phone, deleted)
            VALUES ('Ivan', 'Ivanov', 'ivan1@example.com', '+375336402547', false);
            """;

    public static final PassengerDto PASSENGER_REQUEST_CREATE_DTO = new PassengerDto(
            null,
            PASSENGER_FIRST_NAME_FOR_CREATE,
            PASSENGER_LAST_NAME_FOR_CREATE,
            PASSENGER_EMAIL_FOR_CREATE,
            PASSENGER_PHONE_FOR_CREATE_UPDATE,
            null,
            null);

    public static final PassengerDto PASSENGER_RESPONSE_CREATE_DTO = new PassengerDto(
            ID_AFTER_CREATE,
            PASSENGER_FIRST_NAME_FOR_CREATE,
            PASSENGER_LAST_NAME_FOR_CREATE,
            PASSENGER_EMAIL_FOR_CREATE,
            PASSENGER_PHONE_FOR_CREATE_UPDATE,
            null,
            false);

    public static final PassengerDto PASSENGER_REQUEST_UPDATE_DTO = new PassengerDto(
            null,
            null,
            null,
            null,
            PASSENGER_PHONE_FOR_CREATE_UPDATE,
            null,
            null);

    public static final PassengerDto PASSENGER_RESPONSE_UPDATE_DTO = new PassengerDto(
            ID,
            PASSENGER_FIRST_NAME_FOR_GET_UPDATE,
            PASSENGER_LAST_NAME_FOR_GET_UPDATE,
            PASSENGER_EMAIL_FOR_GET_UPDATE,
            PASSENGER_PHONE_FOR_CREATE_UPDATE,
            null,
            false);

    public static final PassengerDto PASSENGER_RESPONSE_GET_DTO = new PassengerDto(
            ID,
            PASSENGER_FIRST_NAME_FOR_GET_UPDATE,
            PASSENGER_LAST_NAME_FOR_GET_UPDATE,
            PASSENGER_EMAIL_FOR_GET_UPDATE,
            PASSENGER_PHONE_FOR_GET,
            null,
            false);

    public static final ListContainerResponseDto<PassengerDto> PASSENGER_PAGE_GET_DTO =
            ListContainerResponseDto.<PassengerDto>builder()
                    .withSort(SORT_TYPE)
                    .withTotalElements(TOTAL_ELEMENTS)
                    .withCurrentLimit(CURRENT_LIMIT)
                    .withCurrentOffset(CURRENT_OFFSET)
                    .withTotalPages(TOTAL_PAGES)
                    .withValues(List.of(PASSENGER_RESPONSE_GET_DTO))
                    .build();

}