package com.modsen.driverservice;

import com.modsen.driverservice.dto.CarDto;
import com.modsen.driverservice.dto.DriverCarDto;
import com.modsen.driverservice.dto.DriverDto;
import com.modsen.driverservice.dto.ListContainerResponseDto;

import java.util.List;

public class AppIntegrationTestUtil {

    public static final Long ID = 1L;
    private static final long ID_AFTER_CREATE = 2L;

    private static final String CAR_UPDATE_BRAND = "Toyota";
    private static final String CAR_UPDATE_COLOR = "Pink";
    private static final String CAR_UPDATE_NUMBER = "ABC123";
    private static final String CAR_UPDATE_MODEL = "Toyota Camry";
    private static final int CAR_UPDATE_YEAR = 2020;

    private static final String CAR_CREATE_BRAND = "BMW";
    private static final String CAR_CREATE_COLOR = "Black";
    private static final String CAR_CREATE_NUMBER = "ABC321";
    private static final String CAR_CREATE_MODEL = "E60";
    private static final int CAR_CREATE_YEAR = 2004;

    private static final String CAR_GET_BRAND = "Toyota";
    private static final String CAR_GET_COLOR = "Red";
    private static final String CAR_GET_NUMBER = "ABC123";
    private static final String CAR_GET_MODEL = "Toyota Camry";
    private static final int CAR_GET_YEAR = 2020;

    private static final String DRIVER_GET_NAME = "John Doe";
    private static final String DRIVER_GET_EMAIL = "johndoe@example.com";
    private static final String DRIVER_GET_PHONE = "+1234567890";
    private static final String DRIVER_GET_GENDER = "Male";

    private static final String DRIVER_CREATE_NAME = "John Lock";
    private static final String DRIVER_CREATE_EMAIL = "john.lock@gmail.com";
    private static final String DRIVER_CREATE_PHONE = "+1231231231";

    private static final String DRIVER_UPDATE_NAME = "Jack Shepherd";
    private static final String DRIVER_UPDATE_EMAIL = "johndoe@example.com";
    private static final String DRIVER_UPDATE_PHONE = "+1234567890";

    private static final String TYPE_OF_SORT = "UNSORTED";

    public static final String SQL_DELETE_ALL_DATA = """
            DELETE FROM car;
            DELETE FROM driver;
            """;

    public static final String SQL_RESTART_SEQUENCES = """
            ALTER SEQUENCE car_id_seq RESTART WITH 1;
            ALTER SEQUENCE driver_id_seq RESTART WITH 1;
            """;

    public static final String SQL_INSERT_CAR = """
            INSERT INTO driver (name, email, phone, gender, deleted)
            VALUES ('John Doe', 'johndoe@example.com', '+1234567890', 'Male', false);
            
            INSERT INTO car (brand, color, number, model, year, driver_id, deleted)
            VALUES ('Toyota', 'Red', 'ABC123', 'Toyota Camry', 2020, 1, false);
            """;

    // Car UPDATE endpoint
    public static final CarDto CAR_UPDATE_REQUEST_DTO = new CarDto(
            null,
            null,
            CAR_UPDATE_COLOR,
            null,
            null,
            null,
            null);

    public static final CarDto CAR_UPDATE_RESPONSE_DTO = new CarDto(
            ID,
            CAR_UPDATE_BRAND,
            CAR_UPDATE_COLOR,
            CAR_UPDATE_NUMBER,
            CAR_UPDATE_MODEL,
            CAR_UPDATE_YEAR,
            false);

    // Car CREATE endpoint
    public static final CarDto CAR_CREATE_REQUEST_DTO = new CarDto(
            null,
            CAR_CREATE_BRAND,
            CAR_CREATE_COLOR,
            CAR_CREATE_NUMBER,
            CAR_CREATE_MODEL,
            CAR_CREATE_YEAR,
            null);
    public static final CarDto CAR_CREATE_RESPONSE_DTO = new CarDto(
            ID_AFTER_CREATE,
            CAR_CREATE_BRAND,
            CAR_CREATE_COLOR,
            CAR_CREATE_NUMBER,
            CAR_CREATE_MODEL,
            CAR_CREATE_YEAR,
            false);

    // Car GET endpoint
    public static final CarDto CAR_GET_RESPONSE_DTO = new CarDto(
            ID,
            CAR_GET_BRAND,
            CAR_GET_COLOR,
            CAR_GET_NUMBER,
            CAR_GET_MODEL,
            CAR_GET_YEAR,
            false);

    // Driver CREATE endpoint
    public static final DriverDto DRIVER_CREATE_REQUEST_DTO = new DriverDto(
            null,
            DRIVER_CREATE_NAME,
            DRIVER_CREATE_EMAIL,
            DRIVER_CREATE_PHONE,
            DRIVER_GET_GENDER,
            null,
            null);

    public static final DriverDto DRIVER_CREATE_RESPONSE_DTO = new DriverDto(
            ID_AFTER_CREATE,
            DRIVER_CREATE_NAME,
            DRIVER_CREATE_EMAIL,
            DRIVER_CREATE_PHONE,
            DRIVER_GET_GENDER,
            null,
            false);

    // Driver UPDATE endpoint
    public static final DriverDto DRIVER_UPDATE_REQUEST_DTO = new DriverDto(
            null,
            DRIVER_UPDATE_NAME,
            null,
            null,
            null,
            null,
            null);

    public static final DriverDto DRIVER_UPDATE_RESPONSE_DTO = new DriverDto(
            ID,
            DRIVER_UPDATE_NAME,
            DRIVER_UPDATE_EMAIL,
            DRIVER_UPDATE_PHONE,
            DRIVER_GET_GENDER,
            null,
            false);

    // Driver GET_BY_ID endpoint
    public static final DriverDto DRIVER_GET_RESPONSE_DTO = new DriverDto(
            ID,
            DRIVER_GET_NAME,
            DRIVER_GET_EMAIL,
            DRIVER_GET_PHONE,
            DRIVER_GET_GENDER,
            null,
            false);

    // Driver GET with PAGE endpoint
    public static final ListContainerResponseDto<DriverDto> DRIVER_PAGE_GET_RESPONSE_DTO =
            ListContainerResponseDto.<DriverDto>builder()
                    .withTotalPages(1)
                    .withCurrentLimit(10)
                    .withCurrentOffset(0)
                    .withSort(TYPE_OF_SORT)
                    .withTotalElements(1)
                    .withValues(List.of(DRIVER_GET_RESPONSE_DTO))
                    .build();

    // Driver GET_WITH_CAR endpoint
    public static final DriverCarDto DRIVER_CAR_RESPONSE_DTO = new DriverCarDto(
            DRIVER_GET_RESPONSE_DTO.id(),
            DRIVER_GET_RESPONSE_DTO.name(),
            DRIVER_GET_RESPONSE_DTO.email(),
            DRIVER_GET_RESPONSE_DTO.phone(),
            DRIVER_GET_RESPONSE_DTO.gender(),
            DRIVER_GET_RESPONSE_DTO.averageRating(),
            List.of(CAR_GET_RESPONSE_DTO));

}