package com.modsen.driverservice;

import com.modsen.driverservice.dto.CarDto;
import com.modsen.driverservice.dto.DriverCarDto;
import com.modsen.driverservice.dto.DriverDto;
import com.modsen.driverservice.dto.ListContainerResponseDto;
import com.modsen.driverservice.model.Car;
import com.modsen.driverservice.model.Driver;

import java.util.ArrayList;
import java.util.Collections;

public class AppTestUtil {

    // For controllers tests
    public static final Long DRIVER_ID = 1L;
    public static final Long CAR_ID = 1L;

    public static final String CAR_ENDPOINT = "/api/v1/cars/drivers/{driverId}";
    public static final String CAR_UPDATE_ENDPOINT = "/api/v1/cars/{carId}/drivers/{driverId}";
    public static final String CAR_DELETE_ENDPOINT = "/api/v1/cars/{carId}";
    public static final String CAR_GET_ENDPOINT = "/api/v1/cars/{carId}";

    public static final String DRIVER_ENDPOINT = "/api/v1/drivers";
    public static final String DRIVER_UPDATE_ENDPOINT = "/api/v1/drivers/{driverId}";
    public static final String DRIVER_DELETE_ENDPOINT = "/api/v1/drivers/{driverId}";
    public static final String DRIVER_GET_ENDPOINT = "/api/v1/drivers/{driverId}";
    public static final String DRIVER_CARS_ENDPOINT = "/api/v1/drivers/{driverId}/cars";

    public static final CarDto carResponseDto = new CarDto(
            CAR_ID,
            "Brand",
            "Color",
            "ABC123",
            "Model",
            2020,
            false);

    public static final CarDto carRequestDto = new CarDto(
            null,
            "Brand",
            "Color",
            "ABC123",
            "Model",
            2020,
            null);

    public static final CarDto invalidCarRequestDto = new CarDto(
            null,
            "Brand",
            "Color",
            "ABC123ABC123",
            "Model",
            2020,
            true
    );

    public static final DriverDto driverResponseDto = new DriverDto(
            DRIVER_ID,
            "Name",
            "jane.doe@example.org",
            "6625550144",
            "Gender",
            5.0d,
            false);

    public static final DriverDto driverRequestDto = new DriverDto(
            null,
            "Name",
            "jane.doe@example.org",
            "6625550144",
            "Gender",
            null,
            false
    );

    public static final DriverDto invalidDriverRequestDto = new DriverDto(
            null,
            "Name",
            "",
            "",
            "Gender",
            5d,
            true
    );

    public static final DriverCarDto driverCarResponseDto = new DriverCarDto(
            1L,
            "Name",
            "jane.doe@example.org",
            "6625550144",
            "Gender",
            5.0d,
            Collections.singletonList(carResponseDto));

    public static final ListContainerResponseDto<DriverDto> pageDriverResponseDto = new ListContainerResponseDto<>(
            1, 1, 1, 1L,
            "Sort", new ArrayList<>());

    // For services tests
    public static final Driver driverEntity;
    public static final Car carEntity;
    public static final CarDto carDto;
    public static final DriverDto driverDto;

    static {
        Long anyId = 1L;

        driverEntity = new Driver();
        driverEntity.setAverageRating(10.0d);
        driverEntity.setCars(new ArrayList<>());
        driverEntity.setDeleted(true);
        driverEntity.setEmail("jane.doe@example.org");
        driverEntity.setGender("Gender");
        driverEntity.setId(anyId);
        driverEntity.setName("Name");
        driverEntity.setPhone("6625550144");

        carEntity = new Car();
        carEntity.setBrand("Brand");
        carEntity.setColor("Color");
        carEntity.setDeleted(true);
        carEntity.setDriver(driverEntity);
        carEntity.setId(anyId);
        carEntity.setModel("Model");
        carEntity.setNumber("42");
        carEntity.setYear(1);

        carDto = new CarDto(anyId, "Brand", "Color", "42", "Model", 1, true);
        driverDto = new DriverDto(anyId, "Name", "jane.doe@example.org",
                "6625550144", "Gender", 10.0d, true);
    }

}