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

class SeleniumforDoctor {
	private static FirefoxDriver driver;

	@BeforeAll
	public static void setUp() {
		// add options to firefox driver
		FirefoxOptions options = new FirefoxOptions();

		driver = new FirefoxDriver(options);
	}

	@AfterAll
	public static void tearDown() {
//		Quit browser
		driver.quit();
	}

	@Test
	  public void seleniumTest() {
		    driver.get("http://localhost:8080/doctor/register");
		    driver.manage().window().setSize(new Dimension(550, 692));
		    driver.findElement(By.id("firstName")).click();
		    driver.findElement(By.id("firstName")).sendKeys("Thanh Hung");
		    driver.findElement(By.id("lastName")).click();
		    driver.findElement(By.id("lastName")).sendKeys("Do");
		    driver.findElement(By.id("email")).click();
		    driver.findElement(By.id("email")).sendKeys("thanhhung110999@gmail.com");
		    driver.findElement(By.cssSelector("body")).click();
		    driver.findElement(By.id("phoneNumber")).click();
		    driver.findElement(By.id("phoneNumber")).sendKeys("7788898989");
		    driver.findElement(By.id("dateOfBirth")).click();
		    driver.findElement(By.id("dateOfBirth")).sendKeys("11091999");
		    driver.findElement(By.id("password")).click();
		    driver.findElement(By.id("password")).sendKeys("abcxyz123789");
		    driver.findElement(By.id("confirmPassword")).click();
		    driver.findElement(By.id("confirmPassword")).sendKeys("abcxyz123789");
		    driver.findElement(By.cssSelector("input:nth-child(15)")).click();
		    driver.findElement(By.id("password")).click();
		    driver.findElement(By.id("dateOfBirth")).click();
		    driver.findElement(By.id("dateOfBirth")).click();
		    driver.findElement(By.id("dateOfBirth")).click();
		    driver.findElement(By.id("dateOfBirth")).sendKeys("11/09/1999");
		    driver.findElement(By.id("password")).click();
		    driver.findElement(By.id("password")).sendKeys("Abcxyz123789");
		    driver.findElement(By.id("confirmPassword")).click();
		    driver.findElement(By.id("confirmPassword")).sendKeys("Abcxyz123789");
		    driver.findElement(By.cssSelector("input:nth-child(15)")).click();
		    driver.findElement(By.id("email")).click();
		    driver.findElement(By.id("email")).sendKeys("thanhhung110999@gmail.com");
		    driver.findElement(By.id("password")).click();
		    driver.findElement(By.id("password")).sendKeys("Abcxyz123789");
		    driver.findElement(By.cssSelector("input:nth-child(5)")).click();
		  }

}