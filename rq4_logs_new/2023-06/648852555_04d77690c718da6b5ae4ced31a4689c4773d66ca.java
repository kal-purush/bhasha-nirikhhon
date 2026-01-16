package com.cydeo.tests.day02_locators_getText_getAttribute;

import com.google.common.base.Verify;
import io.github.bonigarcia.wdm.WebDriverManager;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;

public class T1_CydeoVerifications {

    public static void main(String[] args) {

//        TC #1: Cydeo practice tool verifications
//            1. Open Chrome browser

        WebDriverManager.chromedriver().setup();
        WebDriver driver = new ChromeDriver();
        driver.manage().window().maximize();

//            2. Go to https://practice.cydeo.com

        driver.get("https://practice.cydeo.com");

//            3. Verify URL contains
//                Expected: cydeo

        String expectedURL = "cydeo";
        String actualURL = driver.getCurrentUrl();

        if (actualURL.contains(expectedURL)) {
            System.out.println("AC1: URL " + driver.getCurrentUrl() + " contains cydeo");
        } else {
            System.err.println("AC1:URL " + driver.getCurrentUrl() + " DOES NOT CONTAIN cydeo");
        }

//            4. Verify title:
//                Expected: Practice

        String expectedTitle = "Practice";
        String actualTitle = driver.getTitle();

        if (actualTitle.equals(expectedTitle)) {
            System.out.println("AC2: Title matches required criteria: Practice");
        } else {
            System.err.println("AC2: Title DOES NOT match the required criteria: Practice");
        }

        driver.quit();


    }


}