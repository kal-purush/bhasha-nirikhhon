package com.DemoTest.Tests;


import com.saucelabs.common.SauceOnDemandAuthentication;
import com.saucelabs.common.SauceOnDemandSessionIdProvider;
import com.saucelabs.testng.SauceOnDemandAuthenticationProvider;
import com.saucelabs.testng.SauceOnDemandTestListener;

import org.openqa.selenium.Alert;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.remote.CapabilityType;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;

import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.UnexpectedException;

public class AlertDemo {
	
	 public static final String TestURL = "http://demo.guru99.com/test/delete_customer.php";
	 public static final String USERNAME = "iflanagan";
	  public static final String ACCESS_KEY = "4513840c-236b-4045-86bd-88e0c0ebfb50";
	  public static final String URL = "https://" + USERNAME + ":" + ACCESS_KEY + "@ondemand.saucelabs.com:443/wd/hub";
	 
	  public static void main(String[] args) throws Exception {
	 
	    DesiredCapabilities caps = DesiredCapabilities.chrome();
	    caps.setCapability("platform", "Windows 10");
	    caps.setCapability("browserName", "chrome");
	    caps.setCapability("version", "latest");
	 
	    WebDriver driver = new RemoteWebDriver(new URL(URL), caps);
	    
	    try
	    {

	    driver.get(TestURL);			
		
	      	
        driver.findElement(By.name("cusid")).sendKeys("53920");					
        driver.findElement(By.name("submit")).submit();			
        		
        // Switching to Alert        
        Alert alert = driver.switchTo().alert();		
        		
        // Capturing alert message.    
        String alertMessage= driver.switchTo().alert().getText();		
        		
        // Displaying alert message		
        System.out.println(alertMessage);	
        Thread.sleep(5000);
        
	    
        		
        // Accepting alert		
        alert.accept();	
        
        
        driver.close();
	 
	    driver.quit();
	    
	    }
	    
	    catch (Exception ex)
	    {
	    	 System.out.println("\nCan't execute script on SauceLabs: " +ex);	
	    }

}
}