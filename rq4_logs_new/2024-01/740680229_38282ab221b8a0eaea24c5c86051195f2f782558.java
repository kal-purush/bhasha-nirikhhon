package com.XFleet.pages;

import com.XFleet.utilities.Driver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class VehicleModelPage {
    public VehicleModelPage(){
        PageFactory.initElements(Driver.getDriver(),this);
    }

    @FindBy(xpath = "//li[@class='dropdown dropdown-level-1']")
    public WebElement fleet;
    //span[@class='title title-level-1']//i[@class='fa-asterisk menu-icon']
    @FindBy(xpath = "//span[text()='Vehicles Model']")
    public WebElement vehiclesModel;


    @FindBy(xpath = "//div[.='You do not have permission to perform this action.']")
    public WebElement warningMessage;

}