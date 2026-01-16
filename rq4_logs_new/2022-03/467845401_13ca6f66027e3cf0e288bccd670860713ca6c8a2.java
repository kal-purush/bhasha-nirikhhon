package com.uiElemints;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

public class Handling {

	public static void main(String[] args) {
		WebDriver driver;
		System.setProperty("webdriver.chrome.driver","/Users/anilkumarreddy/Documents/chrome driver/chromedriver");
 driver=new ChromeDriver();
 driver.get("https://mail.rediff.com/cgi-bin/login.cgi");
driver.manage().window().maximize();
WebElement cb=driver.findElement(By.id("remember"));
if(cb.isDisplayed())
if(cb.isEnabled())
	if(cb.isSelected()==true)
		cb.click();
	
	}

}