package com.sample.samplescripts;

import org.openqa.selenium.By;
import org.openqa.selenium.chrome.ChromeDriver;

public class GaanaSearch 
{

	public static void main(String[] args) throws InterruptedException 
	{
		System.setProperty("webdriver.chrome.driver", "./drivers/chromedriver.exe");
		ChromeDriver driver = new ChromeDriver();
		driver.manage().window().maximize();
		
		
		driver.get("https://gaana.com/");
		
		driver.findElement(By.id("searchTop")).click();
		Thread.sleep(1000);
		
		driver.findElement(By.id("sb")).sendKeys("eminem");
		driver.findElement(By.className("search_btn")).click();
		
	}

}