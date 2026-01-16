package com.epam.brest.summer.courses2019.model;

/**
 * POJO Car for model.
 */
public class Car {

    /**
     * Car Id.
     */
    private Integer carId;

    /**
     * Car Model.
     */
    private String carModel;

    /**
     * Car Number.
     */
    private String carNumber;

    /**
     * Car Load Capacity.
     */
    private Integer loadCapacity;

    /**
     * Car Сharacteristics.
     */
    private String carСharacteristics;

    /**
     * Car Driver.
     */
    private String carDriver;

    /**
     * Car is Fixed.
     */
    private Boolean isFixed;

    public void setCarId(Integer carId) {
        this.carId = carId;
    }

    public void setCarModel(String carModel) {
        this.carModel = carModel;
    }

    public void setCarNumber(String carNumber) {
        this.carNumber = carNumber;
    }

    public void setLoadCapacity(Integer loadCapacity) {
        this.loadCapacity = loadCapacity;
    }

    public void setCarСharacteristics(String carСharacteristics) {
        this.carСharacteristics = carСharacteristics;
    }

    public void setCarDriver(String carDriver) {
        this.carDriver = carDriver;
    }

    public void setFixed(Boolean fixed) {
        isFixed = fixed;
    }

    public Integer getCarId() {
        return carId;
    }

    public String getCarModel() {
        return carModel;
    }

    public String getCarNumber() {
        return carNumber;
    }

    public Integer getLoadCapacity() {
        return loadCapacity;
    }

    public String getCarСharacteristics() {
        return carСharacteristics;
    }

    public String getCarDriver() {
        return carDriver;
    }

    public Boolean getFixed() {
        return isFixed;
    }
}