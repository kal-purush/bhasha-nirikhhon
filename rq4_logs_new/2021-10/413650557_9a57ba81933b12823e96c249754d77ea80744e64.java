package com.vytrack.pages;

import com.trycloud.utilities.Driver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class CarPage {

    @FindBy(css = ".control-group:nth-child(2) div.control-label")
    private WebElement licensePlate;

    @FindBy(css = ".control-group:nth-child(3) div.control-label")
    private WebElement tags;

    @FindBy(css = ".control-group:nth-child(4) div.control-label")
    private WebElement driver;

    @FindBy(css = ".control-group:nth-child(5) div.control-label")
    private WebElement location;

    @FindBy(css = ".control-group:nth-child(6) div.control-label")
    private WebElement chassisNumber;

    @FindBy(css = ".control-group:nth-child(7) div.control-label")
    private WebElement modelYear;

    @FindBy(css = ".control-group:nth-child(8) div.control-label")
    private WebElement lastOdometer;

    @FindBy(css = ".control-group:nth-child(9) div.control-label")
    private WebElement immatriculationDate;

    @FindBy(css = ".control-group:nth-child(10) div.control-label")
    private WebElement firstContractDate;

    @FindBy(css = ".control-group:nth-child(11) div.control-label")
    private WebElement catalogValueVat;

    @FindBy(css = ".control-group:nth-child(12) div.control-label")
    private WebElement seatsNumber;

    @FindBy(css = ".control-group:nth-child(13) div.control-label")
    private WebElement doorsNumber;

    @FindBy(css = ".control-group:nth-child(14) div.control-label")
    private WebElement colorNumber;

    @FindBy(css = ".control-group:nth-child(15) div.control-label")
    private WebElement transmissionNumber;

    @FindBy(css = ".control-group:nth-child(16) div.control-label")
    private WebElement fuelType;

    @FindBy(css = ".control-group:nth-child(17) div.control-label")
    private WebElement co2Emissions;

    @FindBy(css = ".control-group:nth-child(18) div.control-label")
    private WebElement horsePower;

    @FindBy(css = ".control-group:nth-child(19) div.control-label")
    private WebElement horsePowerTaxation;

    @FindBy(css = ".control-group:nth-child(20) div.control-label")
    private WebElement power;

    @FindBy(css = ".control-group:nth-child(21) div.control-label")
    private WebElement vehicleModel;

    @FindBy(css = ".control-group:nth-child(22) div.control-label")
    private WebElement vehicleMake;


    public String licensePlateText() {
        return licensePlate.getText();
    }

    public String tagsText() {
        return tags.getText();
    }

    public String driverText() {
        return driver.getText();
    }

    public String locationText() {
        return location.getText();
    }

    public String chassisNumberText() {
        return chassisNumber.getText();
    }

    public String modelYearText() {
        return modelYear.getText();
    }

    public String lastOdometerText() {
        return lastOdometer.getText();
    }

    public String immatriculationDateText() {
        return immatriculationDate.getText();
    }

    public String firstContractDateText() {
        return firstContractDate.getText();
    }

    public String catalogValueVatText() {
        return catalogValueVat.getText();
    }

    public String seatsNumberText() {
        return seatsNumber.getText();
    }

    public String doorsNumberText() {
        return doorsNumber.getText();
    }

    public String colorNumberText() {
        return colorNumber.getText();
    }

    public String transmissionNumberText() {
        return transmissionNumber.getText();
    }

    public String fuelTypeText() {
        return fuelType.getText();
    }

    public String co2EmissionsText() {
        return co2Emissions.getText();
    }

    public String horsePowerText() {
        return horsePower.getText();
    }

    public String horsePowerTaxationText() {
        return horsePowerTaxation.getText();
    }

    public String powerText() {
        return power.getText();
    }

    public String vehicleModelText() {
        return vehicleModel.getText();
    };

    public String vehicleMakeText() {
        return vehicleMake.getText();
    };


    public CarPage(){
        PageFactory.initElements(Driver.getDriver(), this);
    }
}

