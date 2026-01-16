package com.csis3275.test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.Dimension;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;




class SeleniumTest {
private static FirefoxDriver driver;
	
	@BeforeAll
	public static void setUp()	{
		//add options to firefox driver
		FirefoxOptions options = new FirefoxOptions();
		
		driver = new FirefoxDriver(options);
	}
	
	@AfterAll
	public static void tearDown()	{
//		Quit browser
		driver.quit();
	}
	
	 
	 @Test
	  public void seleniumTest() {
	    driver.get("http://localhost:8080/patients/list");
	    driver.manage().window().setSize(new Dimension(1721, 1041));
	    driver.findElement(By.linkText("Add New Patient")).click();
	    driver.findElement(By.id("firstName")).click();
	    driver.findElement(By.id("firstName")).sendKeys("John");
	    driver.findElement(By.id("lastName")).click();
	    driver.findElement(By.id("lastName")).sendKeys("Smith");
	    driver.findElement(By.id("email")).click();
	    driver.findElement(By.id("email")).sendKeys("111@222");
	    driver.findElement(By.id("height")).click();
	    driver.findElement(By.id("height")).sendKeys("175");
	    driver.findElement(By.id("weight")).click();
	    driver.findElement(By.id("weight")).sendKeys("75");
	    {
	      WebElement dropdown = driver.findElement(By.id("symptoms"));
	      dropdown.findElement(By.xpath("//option[. = 'Cough']")).click();
	    }
	    {
	      WebElement dropdown = driver.findElement(By.id("symptoms"));
	      dropdown.findElement(By.xpath("//option[. = 'Fever']")).click();
	    }
	    driver.findElement(By.cssSelector("input:nth-child(15)")).click();
	    driver.findElement(By.cssSelector("tr:nth-child(2) .btn-warning")).click();
	    driver.findElement(By.id("height")).click();
	    driver.findElement(By.id("height")).sendKeys("180");
	    driver.findElement(By.id("weight")).click();
	    driver.findElement(By.id("weight")).sendKeys("80");
	    {
	      WebElement dropdown = driver.findElement(By.id("symptoms"));
	      dropdown.findElement(By.xpath("//option[. = 'Headache']")).click();
	    }
	    {
	      WebElement dropdown = driver.findElement(By.id("symptoms"));
	      dropdown.findElement(By.xpath("//option[. = 'Cough']")).click();
	    }
	    {
	      WebElement dropdown = driver.findElement(By.id("symptoms"));
	      dropdown.findElement(By.xpath("//option[. = 'Fever']")).click();
	    }
	    driver.findElement(By.cssSelector("input:nth-child(18)")).click();
	    driver.findElement(By.cssSelector("tr:nth-child(2) .btn-danger")).click();
	  }

	

}