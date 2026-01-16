package com.cydeo.base;

import com.cydeo.utilities.ConfigReader;
import com.cydeo.utilities.WebDriverFactory;
import org.openqa.selenium.WebDriver;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.time.Duration;

//This clas is the parent of all Test classes
//We will update this class based on what you keep writing in most of yout Test classes
//instead of writing in all Test classes, you can just write in this class and extend to Child class
//This class is abstract becasue we want to precent creation of an object.
public abstract class TestBase {

    protected   WebDriver driver;

    @BeforeClass
        public void setup(){

        driver = WebDriverFactory.getDriver(ConfigReader.getProperty("browser"));
        driver.manage().window().maximize();
        driver.manage().timeouts().implicitlyWait(Duration.ofSeconds(10));
        driver.get(ConfigReader.getProperty("env"));
    }

  // @AfterClass
  // public void tearDownMethod(){
   //     driver.quit();
  // }

}