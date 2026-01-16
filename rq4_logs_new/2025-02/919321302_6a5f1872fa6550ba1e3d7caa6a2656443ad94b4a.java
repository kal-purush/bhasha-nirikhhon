package org.automation.pageobjects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.automation.constants.LoginDetails;
import org.automation.framework.BrowserManager;
import org.automation.framework.SeleniumActions;
import org.openqa.selenium.By;

public class LoginPage {

    Logger log = LogManager.getRootLogger();
    BrowserManager manager = new BrowserManager();
    SeleniumActions actions = new SeleniumActions(manager);

    private final static By EMAIL_FIELD = By.xpath("//input[@id='email']");
    private final static By PASSWORD_FIELD = By.xpath("//input[@id='pass']");
    private final static By SUBMIT_BUTTON = By.xpath("//button[@type='submit']");

    public void openLoginPage() {
        log.info("Open Classic Football Shirts myaccount page");
        manager.openBrowser();
        manager.getDriver().get("https://www.classicfootballshirts.co.uk/customer/account/login/referer/aHR0cHM6Ly93d3cuY2xhc3NpY2Zvb3RiYWxsc2hpcnRzLmNvLnVrL2N1c3RvbWVyL2FjY291bnQvaW5kZXgv/");
        manager.getDriver().manage().window().maximize();
    }

    public void loginPage() {
        log.info("Login to Classic Football Shirts account");
        String user = LoginDetails.LOGIN_EMAIL.getEmail();
        String pass = LoginDetails.LOGIN_EMAIL.getPassword();

        actions.waitElementToBeClickable(EMAIL_FIELD, 12);
        actions.sendKeys(EMAIL_FIELD, user);
        actions.sendKeys(PASSWORD_FIELD, pass);
        actions.clickElement(SUBMIT_BUTTON);
    }


}