package com.hemant.base;

import java.util.concurrent.TimeUnit;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;

import com.aventstack.extentreports.ExtentReports;
import com.aventstack.extentreports.ExtentTest;
import com.aventstack.extentreports.reporter.ExtentHtmlReporter;
import com.beust.jcommander.Parameter;

import io.github.bonigarcia.wdm.WebDriverManager;

public class Main {

	public  static WebDriver driver;
	public  ExtentHtmlReporter htmlReporter = new ExtentHtmlReporter("./reports/test.html");
	public  static ExtentReports extent;
	public  static ExtentTest logger;

	public  void init() {
		extent = new ExtentReports();
		extent.attachReporter(htmlReporter);
	}

	@BeforeSuite
	public void beforeSuite() {
		init();
		System.out.println("Inside Before Suite");
	}

	@BeforeTest
	@Parameters({ "browser" })
	public void beforeTest(String browser) {
		if (browser.equals("Chrome")) {
			WebDriverManager.chromedriver().setup();
			driver = new ChromeDriver();
		} else if (browser.equals("Firefox")) {
			WebDriverManager.firefoxdriver().setup();
			driver = new FirefoxDriver();
		}
		driver.manage().timeouts().pageLoadTimeout(30, TimeUnit.SECONDS);
		System.out.println("Inside Before Test");
	}

//	@BeforeClass
	public void beforeClass() {
		System.out.println("Inside Before Class");
	}

	@AfterClass
	public void afterClass() {
		System.out.println("Inside After Class");
		driver.close();
	}

//	@AfterTest
	public void afterTest() {
		System.out.println("Inside After Test");
	}

//	@AfterSuite
	public void afterSuite() {
		System.out.println("Inside After Suite");
	}

}