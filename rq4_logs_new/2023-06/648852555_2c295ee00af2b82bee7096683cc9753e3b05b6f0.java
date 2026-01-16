package com.cydeo.tests.day09_LAPTOP_javaFaker;

import com.cydeo.utilities.BrowserUtils;
import com.cydeo.utilities.ConfigReader;
import com.cydeo.utilities.WebDriverFactory;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Duration;

public class T1_Bing_Search {

    WebDriver driver;

    @BeforeClass
    public void setupClass() {
        driver = WebDriverFactory.getDriver(ConfigReader.getProperty("browser"));
        driver.manage().window().maximize();
        driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(10));

        driver.get(ConfigReader.getProperty("env"));

    }

    @Test(priority = 1)
    public void bingSearch() {

        WebElement searchBox = driver.findElement(By.xpath("//textarea[@type='search']"));
        BrowserUtils.sleep(2);
        searchBox.sendKeys(ConfigReader.getProperty("searchKeyword") + Keys.ENTER);

        BrowserUtils.sleep(2);
        BrowserUtils.verifyTitle(driver, ConfigReader.getProperty("searchKeyword") + " - Search");
    }

}