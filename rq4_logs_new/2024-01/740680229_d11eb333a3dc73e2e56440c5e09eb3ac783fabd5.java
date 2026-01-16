package com.XFleet.pages;

import com.XFleet.utilities.Driver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class VehicleContractsPage {

    public VehicleContractsPage(){
        PageFactory.initElements(Driver.getDriver(),this);
    }

    @FindBy(xpath = "//li[@class='dropdown dropdown-level-1']")
    public WebElement fleet;

    @FindBy(xpath = "//span[text()='Vehicle Contracts']")
    public WebElement vehicCont;


    @FindBy(xpath = "(//div[@class='alert alert-error fade in top-messages '])[2]")
    public WebElement warningMessage;


}