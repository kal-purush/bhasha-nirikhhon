package com.XFleet.pages;

import com.XFleet.utilities.Driver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class PinbarPage {

    public PinbarPage(){
        PageFactory.initElements(Driver.getDriver(),this);
    }

    @FindBy(linkText = "Learn how to use this space")
    public WebElement pinBar;

    @FindBy(xpath = "//img[@src='/bundles/oronavigation/images/pinbar-location.jpg']")
    public WebElement pinBarImage;

    @FindBy(xpath ="//button[@title='Pin/unpin the page']")
    public WebElement pinIcon;
}