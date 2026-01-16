package com.example.carbon.love.carbonlove.BaseUtility;

import com.example.carbon.love.carbonlove.Pages.LoginPage;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

import java.util.List;

public class BaseLogin {

    private WebDriver driver;
    private LoginPage loginPage;

    /**
     * Create the constructor stub for the BaseLogin class.
     */

    public BaseLogin(WebDriver driver) {
        this.driver = driver;
    }

    public BaseLogin(LoginPage aLoginPage) {
        this.loginPage = aLoginPage;
    }

    /**
     * Adds the user and password to the login page and clicks on the login button
     * @param userName the username
     * @param password the password
     */
    public void authenticate(String userName, String password) {
        loginPage.addUserName(userName);
        loginPage.addPassword(password);
        loginPage.clickLoginButton();
    }

    /**
     * Adds the user and password to the login page using the STANDARD_USER credentials
     * @return the LoginPage class
     */
    public LoginPage authenticateStandardUser() {
        return authenticateAs(LoginCredentials.STANDARD_USER);
    }

    /**
     * Adds the user and password to the login page using the LOCKED_OUT_USER credentials
     * @return the LoginPage class
     */
    public LoginPage authenticateLockedOutUser() {
        return authenticateAs(LoginCredentials.LOCKED_OUT_USER);
    }

    /**
     * Adds the user and password to the login page using the INVALID_USER credentials
     * @return the LoginPage class
     */
    public LoginPage authenticateInvalidUser() {
        return authenticateAs(LoginCredentials.INVALID_USER);
    }

    /**
     * Authenticates the application using the credentials
     * @param accessCredentials the credentials to use
     * @return the LoginPage class
     */
    private LoginPage authenticateAs(LoginCredentials accessCredentials) {
        authenticate(accessCredentials.getUserName(), accessCredentials.getPassword());
        return new LoginPage(driver);
    }


    /**
     * Create a wrapper for each Selenium methods
     * This is optional, but it helps to keep the code clean.
     */
    public WebElement findElement(By locator){
        return driver.findElement(locator);
    };

    public List<WebElement> findElements(By locator){
        return driver.findElements(locator);
    };

    public void click(By locator){
        driver.findElement(locator).click();
    };

    public void sendKeys(By locator, String inputText){
        driver.findElement(locator).sendKeys(inputText);
    };

    public String getText(By locator){
        return driver.findElement(locator).getText();
    };

    public String getText(WebElement element){
        return element.getText();
    }

    public Boolean isDisplayed(By locator){
        try {
            return driver.findElement(locator).isDisplayed();
        } catch (org.openqa.selenium.NoSuchElementException e) {
            return false;
        }
    }

    public void visit(String url){
        driver.get(url);
    }

    public void quit(){
        driver.quit();
    }
}