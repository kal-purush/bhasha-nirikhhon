package com.XFleet.pages;

import com.XFleet.utilities.ConfigurationReader;
import com.XFleet.utilities.Driver;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

import java.util.List;

public class CheckBoxPage {

    Actions action = new Actions(Driver.getDriver());


    public CheckBoxPage(){
        PageFactory.initElements(Driver.getDriver(), this);
    }

    @FindBy(xpath = "//button")
    public WebElement loginButton;

    @FindBy(xpath = "//a[@href=\"entity/Extend_Entity_Carreservation\"]")
    public WebElement vehicleButton;

    @FindBy(xpath = "//td[@data-column-label=\"Selected Rows\"]")
    public List<WebElement> clickBoxes;

    @FindBy(xpath = "//input[@data-role=\"select-row-cell\"]")
    public List<WebElement> checkBoxes;

    @FindBy(xpath = "(//button[@data-toggle=\"dropdown\"])[2]/input")
    public WebElement firstCheckBox;


//    WebElement resultText = driver.findElement(By.xpath("//div[normalize-space(text()) ='You logged into a secure area!']"));
    @FindBy(xpath = "//span[normalize-space(text()) ='Fleet']/..")
    public WebElement fleetButton;
    LoginPage loginPage = new LoginPage();

    public void login(String userType){

        userType= userType.replace(" ","_");

        String username = ConfigurationReader.getProperty(userType + "_username");
        String password = ConfigurationReader.getProperty(userType + "_password");

        loginPage.login(username,password);

    }

}