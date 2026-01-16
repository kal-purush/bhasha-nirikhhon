package com.test.pages;

import com.test.users.User;
import com.test.utils.ConfigProperties;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;

public class LoginPage extends Page {

    public LoginPage(WebDriver driver) {
        super(driver);
    }

    @FindBy(id = "Email")
    public WebElement email;

    @FindBy(id = "Passwd")
    public WebElement passwd;

    @FindBy(id = "signIn")
    public WebElement signIn;

    public void loginAs(User user) {
        email.sendKeys(user.name);
        passwd.sendKeys(user.password);
        signIn.click();
    }

    @Override
    public void open() {
        driver.get(ConfigProperties.getProperty("url"));
    }
}