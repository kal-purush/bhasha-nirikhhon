package org.maui.selenium.web;

import java.lang.reflect.Method;

import org.maui.selenium.web.factory.WebDriverFactory;
import org.openqa.selenium.WebDriver;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public class BaseTest {
	
	protected static final String BASE_URL = "http://mobile.walmart.com";
	protected WebDriver driver;

    @BeforeClass
    public void setup()  {
        this.driver = WebDriverFactory.prepareWebDriver();
        this.driver.manage().window();
    }
    
    @AfterClass
    public void tearDown() {
        driver.close();
        driver.quit();
    }

    @BeforeMethod
    public void logBeforeTest(Method method) {
        System.out.println("\n******** Starting Test Case: " + method.getName() + " ********\n");
    }

    @AfterMethod
    public void logAfterTest(Method method) {
        System.out.println("\n******** Finished Test Case: " + method.getName() + " ********\n");
    }

    protected WebDriver getDriver() {
        return this.driver;
    }
}