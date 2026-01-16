package com.intetbanking.pageObject;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;

public class LoginPage {
    WebDriver ldriver;

    public LoginPage( WebDriver rdriver)
    {
        ldriver=rdriver;
        PageFactory.initElements(rdriver, this);
    }

    @FindBy(xpath = "//button[@id='details-button']")
    WebElement advancecookiesclick;

    @FindBy(xpath = "//a[@id='proceed-link']")
    WebElement proceedclick;

    @FindBy(name="uid")
    WebElement txtUserName;

    @FindBy(name="password")
    WebElement txtPassword;

    @FindBy(name="btnLogin")
    WebElement btnLogin;

    public void setUserName(String userName)
    {
        txtUserName.sendKeys(userName);
    }

    public void setPassword(String pwd)
    {
        txtPassword.sendKeys(pwd);
    }
    public void clickSubmit()
    {
        btnLogin.click();
    }
    public void clickSadvance()
    {
        advancecookiesclick.click();
    }

    public void clickproceed()
    {
        proceedclick.click();
    }
}