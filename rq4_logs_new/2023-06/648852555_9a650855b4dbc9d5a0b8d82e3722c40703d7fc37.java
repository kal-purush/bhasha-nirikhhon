package com.cydeo.tests.day06_alerts_igrames_windows;

import com.google.common.base.Verify;
import io.github.bonigarcia.wdm.WebDriverManager;
import org.openqa.selenium.Alert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Duration;

public class T1_InformationAlert {

//    TC #1: Information alert practice

    WebDriver driver;
//1. Open browser

    @BeforeClass
    public void setupClass() {
        WebDriverManager.chromedriver().setup();
        driver = new ChromeDriver();
        driver.manage().window().maximize();
        driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(10));

//2. Go to website: http://practice.cydeo.com/javascript_alerts
        driver.get("http://practice.cydeo.com/javascript_alerts");
    }

    @Test(priority = 1)
    public void information_alert_test1() throws InterruptedException {
//3. Click to “Click for JS Alert” button
        WebElement jSButton = driver.findElement(By.xpath("//button[.='Click for JS Alert']"));
        jSButton.click();

//4. Click to OK button from the alert
        Alert popup = driver.switchTo().alert();
        popup.accept();

//5. Verify “You successfully clicked an alert” text is displayed.
        String expectedMessage = "You successfully clicked an alert";
        String actualMessage = driver.findElement(By.id("result")).getText();
        Assert.assertEquals(expectedMessage, actualMessage, "Messages do not match");
    }

    @Test(priority = 2)
    public void accept_alert_test2() throws InterruptedException {
//    3. Click to “Click for JS Confirm” button
        WebElement jsConfirmButton = driver.findElement(By.xpath("//button[@onclick='jsConfirm()']"));
        jsConfirmButton.click();
//    4. Click to OK button from the alert
        Alert jsConfirm = driver.switchTo().alert();
        jsConfirm.accept();

//    5. Verify “You clicked: Ok” text is displayed.
        String expectedTitle2 = "You clicked: Ok";
        String actualTitle2 = driver.findElement(By.id("result")).getText();
        Assert.assertEquals(expectedTitle2, actualTitle2);
    }

    @Test(priority = 3)
    public void prompt_test3() throws InterruptedException {
//    3. Click to “Click for JS Prompt” button
        WebElement jsPromptButton = driver.findElement(By.xpath("//button[@onclick='jsPrompt()']"));
        jsPromptButton.click();

//4. Send “hello” text to alert
        Alert jsConfirm2 = driver.switchTo().alert();
        jsConfirm2.sendKeys("hello");

//5. Click to OK button from the alert
        jsConfirm2.accept();

//6. Verify “You entered: hello” text is displayed.
        String expectedTitle3 = "You entered: hello";
        String actualTitle3 = driver.findElement(By.id("result")).getText();
        Assert.assertEquals(expectedTitle3, actualTitle3);
    }


    @AfterClass
    public void teardownClass() {
        driver.quit();
    }


}