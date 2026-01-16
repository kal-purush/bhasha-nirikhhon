package com.example.backend.pages.almostgame;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

public class AlmostGameMapPage {

    private WebDriver driver;

    @FindBy(how = How.ID, using = "status-button")
    public WebElement statusButton;

    @FindBy(how = How.ID, using = "quit-button")
    public WebElement quitButton;

    @FindBy(how = How.ID, using = "info")
    public WebElement infoText;
    @FindBy(how = How.ID, using = "time")
    public WebElement timeText;
    @FindBy(how = How.ID, using = "round")
    public WebElement roundText;

    @FindBy(how = How.ID, using = "map")
    public WebElement map;



    public AlmostGameMapPage(WebDriver driver) {
        this.driver = driver;
    }

    public void clickStatusButton(){
        statusButton.click();
    }

    public void clickQuitButton(){
        quitButton.click();
    }

    public void clickOnMap(){
        map.click();
    }



}