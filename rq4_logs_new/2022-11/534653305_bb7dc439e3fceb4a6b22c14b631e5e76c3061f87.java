package com.A101.pages;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;

public class AddressPage extends BasePage {

    @FindBy(xpath = "(//a[@title='Yeni adres oluştur'])[1]")
    public WebElement makeNewAddressButton;

    @FindBy(name = "title")
    public WebElement addressTitle;

    @FindBy(name = "township")
    public WebElement township;

    @FindBy(name = "district")
    public WebElement district;

    @FindBy(xpath = "//textarea[@class='js-address-textarea']")
    public WebElement addressBox;

    @FindBy(xpath = "(//button[@type='button'])[6]")
    public WebElement saveButton;

    @FindBy(xpath = "//div[@class='content']/div[2]")
    public WebElement productDescription;

    @FindBy(xpath = "(//button[@type='submit'])[1]")
    public WebElement submitButton;

}