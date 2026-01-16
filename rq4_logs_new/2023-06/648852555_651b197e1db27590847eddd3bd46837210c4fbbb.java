package com.cydeo.tests.day08_LAPTOP;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Duration;

public class T1_WebTablePractice {

//    TC #1: Web table practice
//1. Go to: https://practice.cydeo.com/web-tables

    WebDriver driver;

    @BeforeClass
    public void setupClass() {
        WebDriverManager.chromedriver().setup();
        driver = new ChromeDriver();
        driver.manage().window().maximize();
        driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(10));

        driver.get("https://practice.cydeo.com/web-tables");

    }

    @AfterClass
    public void teardownClass() {
        driver.quit();
    }

    @Test (priority = 1)
    public void verifyName() {

//    2. Verify Bob’s name is listed as expected.
//    Expected: “Bob Martin”
        WebElement bobsName = driver.findElement(By.xpath("(//table[@class='SampleTable']//tr)[7]/td[2]"));

        String expectedName = "Bob Martin";
        String actualName = bobsName.getText();

        Assert.assertEquals(expectedName, actualName);
    }

    @Test (priority = 2)
    public void verifyDate() {

//    3. Verify Bob Martin’s order date is as expected
//    Expected: 12/31/2021
        WebElement bobsName = driver.findElement(By.xpath("(//table[@class='SampleTable']//tr)[7]/td[5]"));

        String expectedName = "12/31/2021";
        String actualName = bobsName.getText();

        Assert.assertEquals(expectedName, actualName);
    }

    @Test (priority = 3)
    public void returnOrderDateUtilTest (){

        WebTableUtils.returnOrderDate(driver, "Bob Martin");

    }

    @Test (priority = 4)
    public void orderVerifyUtilTest (){

        WebTableUtils.orderVerify(driver,"Robert Baratheon","12/04/2021");

    }

}