package com.cydeo.tests.day07_webtables_utilities_javafaker;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;

public class LoginMethod {

    public static void login_crm(WebDriver driver){

    }

    public static void login_crm(WebDriver driver, String username, String password) throws InterruptedException {
        Thread.sleep(2000);
        WebElement loginField = driver.findElement(By.cssSelector("input[placeholder='Login']"));
        loginField.sendKeys(username);

        Thread.sleep(2000);
        WebElement passField = driver.findElement(By.cssSelector("input[placeholder='Password']"));
        passField.sendKeys(password);

        Thread.sleep(2000);
        WebElement loginButton = driver.findElement(By.cssSelector("input[class='login-btn']"));
        loginButton.click();

    }

}