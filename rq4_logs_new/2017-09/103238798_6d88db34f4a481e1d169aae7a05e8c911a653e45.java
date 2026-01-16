package com.coolprodmirror.test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import com.relevantcodes.extentreports.ExtentReports;
import com.relevantcodes.extentreports.ExtentTest;
import com.relevantcodes.extentreports.LogStatus;

public class COOLProdMirrorTest {

	ExtentReports extent;
    ExtentTest test;
    WebDriver driver;
   
    @BeforeTest
    public void init()
    {
    	driver = new FirefoxDriver();
        extent = new ExtentReports(System.getProperty("user.dir") + "/test-output/MonitoringResults.html", true);
    }
     
    @Test
    public void coolStagingTest() throws InterruptedException
    {
        test = extent.startTest("monitoringTest");        
        test.log(LogStatus.PASS, "Browser started");
        driver.get("https://ralbz001097.cloud.dst.ibm.com/login");
        driver.manage().window().maximize();
        driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);        
        WebElement ele=driver.findElement(By.xpath("//a[contains(text(),'IBM intranet password')]"));
        String content=ele.getText();
        System.out.println("Site secured message :  "+content);
        Assert.assertEquals("IBM intranet password", content);
        test.log(LogStatus.PASS, "Site secured by");
        boolean searchEmailPresence = driver.findElement(By.xpath("//input[@name='username']")).isDisplayed();
        boolean searchPasswordPresence = driver.findElement(By.xpath("//input[@name='password']")).isDisplayed();
        
        if (searchEmailPresence==true && searchPasswordPresence==true)
        {
        	WebElement email=driver.findElement(By.xpath("//button[contains(text(),'Sign in')]"));
        	String text=email.getText();
        	System.out.println("Login button test is : "+text);
        	test.log(LogStatus.PASS, "Sign in Button");
        } 
    }
           
    @AfterMethod
    public void getResult(ITestResult result) throws IOException
    {
        if(result.getStatus() == ITestResult.FAILURE)
        {
            String screenShotPath = GetScreenShot.capture(driver, "Monitoring");
            test.log(LogStatus.FAIL, result.getThrowable());
            test.log(LogStatus.FAIL, "Snapshot below: " + test.addScreenCapture(screenShotPath));
        }
        extent.endTest(test);
    }
     
         
    @AfterTest
    public void endreport()
    {
        driver.close();
        extent.flush();
        extent.close();
    }

}