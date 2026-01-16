package com.cydeo.tests.day05_testNG_intro_dropdowns;

import io.github.bonigarcia.wdm.WebDriverManager;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.support.ui.Select;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;

public class T6_SelectAndVerify {

//    TC #6: Selecting date on dropdown and verifying

    WebDriver driver;


    //1. Open Chrome browser
//2. Go to https://practice.cydeo.com/dropdown
    @BeforeMethod
    public void setupMethod() {

        WebDriverManager.chromedriver().setup();
        driver = new ChromeDriver();
        driver.manage().window().maximize();
        driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(10));
        driver.get("https://practice.cydeo.com/dropdown");
    }

//3. Select “December 1st, 1933” and verify it is selected.
//    Select year using : visible text

    @Test
    public void testByText() {

        Select yearByText = new Select(driver.findElement(By.id("year")));
        yearByText.selectByVisibleText("1993");
        Assert.assertTrue(yearByText.getFirstSelectedOption().getText().equals("1993"));

        Select monthByText = new Select(driver.findElement(By.id("month")));
        monthByText.selectByVisibleText("December");
        Assert.assertTrue(monthByText.getFirstSelectedOption().getText().equals("December"));

        Select dayByText = new Select(driver.findElement(By.id("day")));
        dayByText.selectByVisibleText("1");
        Assert.assertTrue(dayByText.getFirstSelectedOption().getText().equals("1"));
    }

//3. Select “December 1st, 1933” and verify it is selected.
//    Select month using : value attribute
    @Test
    public void testByValue() {

        Select yearByValue = new Select(driver.findElement(By.id("year")));
        yearByValue.selectByValue("1993");
        Assert.assertTrue(yearByValue.getFirstSelectedOption().getText().equals("1993"));

        Select monthByValue = new Select(driver.findElement(By.id("month")));
        monthByValue.selectByValue("11");
        Assert.assertTrue(monthByValue.getFirstSelectedOption().getText().equals("December"));

        Select dayByValue = new Select(driver.findElement(By.id("day")));
        dayByValue.selectByValue("1");
        Assert.assertTrue(dayByValue.getFirstSelectedOption().getText().equals("1"));
    }

//3. Select “December 1st, 1933” and verify it is selected.
//    Select day using : index number
    @Test
    public void testByIndex() {

        Select yearByIndex = new Select(driver.findElement(By.id("year")));
        yearByIndex.selectByIndex(30);
        Assert.assertTrue(yearByIndex.getFirstSelectedOption().getText().equals("1993"));

        Select monthByIndex = new Select(driver.findElement(By.id("month")));
        monthByIndex.selectByIndex(11);
        Assert.assertTrue(monthByIndex.getFirstSelectedOption().getText().equals("December"));

        Select dayByIndex = new Select(driver.findElement(By.id("day")));
        dayByIndex.selectByIndex(0);
        Assert.assertTrue(dayByIndex.getFirstSelectedOption().getText().equals("1"));
    }

    @AfterMethod
    public void teardownMethod(){
        driver.quit();
    }
}