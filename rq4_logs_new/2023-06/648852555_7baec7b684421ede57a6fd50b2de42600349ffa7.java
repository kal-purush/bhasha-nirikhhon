package com.cydeo.tests.day02_locators_getText_getAttribute.homework;

import com.google.common.base.Verify;
import io.github.bonigarcia.wdm.WebDriverManager;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

import java.sql.SQLOutput;

public class T1_Etsy {

    public static void main(String[] args) {

        WebDriverManager.chromedriver().setup();

//        HWP #1: Etsy Title Verification
//        1. Open Chrome browser
        WebDriver driver = new ChromeDriver();
        driver.manage().window().maximize();

//        2. Go to https://www.etsy.com
        driver.get("https://www.etsy.com");

//        3. Search for “wooden spoon”
       WebElement searchBar = driver.findElement(By.id("global-enhancements-search-query"));
       searchBar.sendKeys("wooden spoon" + Keys.ENTER);

//        4. Verify title:
//        Expected: “Wooden spoon - Etsy”
        String expectedTitle = "Wooden spoon - Etsy";
        String actualTitle = driver.getTitle();

        if (expectedTitle.equals(actualTitle)){
            System.out.println("Title verification --> PASSED!!!");
        }else{
            System.out.println("Title verification --> FAILED!!!");
        }

        driver.quit();

    }

}