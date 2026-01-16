package com.raghib.selenium;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.chrome.ChromeOptions;
import org.openqa.selenium.edge.EdgeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxOptions;

public class BaseClass {
	
	public static ChromeOptions chromeOptions;
	public static FirefoxOptions firefoxOptions;
	public static WebDriver driver;
	public static String browserArguments1 = "--disable-notifications";
	
	public static WebDriver getDriver(String browserName, String browserVersion) {		
		if(browserName.equalsIgnoreCase("edge")) {
			driver = new EdgeDriver();
		} if(browserName.equalsIgnoreCase("firefox")) {
			firefoxOptions = new FirefoxOptions();
			firefoxOptions.setBrowserVersion(browserVersion);
			firefoxOptions.addArguments(browserArguments1);
			driver = new FirefoxDriver(firefoxOptions);			
		} else if(browserName.equalsIgnoreCase("chrome")) {		
			chromeOptions = new ChromeOptions();
			chromeOptions.setBrowserVersion(browserVersion);
			chromeOptions.addArguments(browserArguments1);
			driver = new ChromeDriver(chromeOptions);
		}
		return driver;		
	}
	
	public static void closeDriver() {
		driver.close();
	}
	
	public static void quitDriver() {
		driver.quit();
	}

}