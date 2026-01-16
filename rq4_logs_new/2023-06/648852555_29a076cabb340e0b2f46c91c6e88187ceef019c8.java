package com.cydeo.tests.day02_locators_getText_getAttribute;

import com.sun.jna.Library;
import io.github.bonigarcia.wdm.WebDriverManager;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

public class T4_LibraryTask {

    public static void main(String[] args) {

        //        1. Open Chrome browser
        WebDriverManager.chromedriver().setup();
        WebDriver driver = new ChromeDriver();
        driver.manage().window().maximize();

//        2. Go to https://library2.cybertekschool.com/login.html
        driver.get("https://library2.cybertekschool.com/login.html");

//        3. Enter username: “incorrect@email.com”
        WebElement username = driver.findElement(By.className("form-control"));
        username.sendKeys("incorrect@email.com");

//        4. Enter password: “incorrect password”
        WebElement password = driver.findElement(By.id("inputPassword"));
        password.sendKeys("incorrect password");

        //locate and click "Sign In Button"
        driver.findElement(By.tagName("button")).click();

//        5. Verify: visually “Sorry, Wrong Email or Password”
//        displayed


//        PS: Locate username input box using “className” locator
//        Locate password input box using “id” locator
//        Locate Sign in button using “tagName” locator


    }

}