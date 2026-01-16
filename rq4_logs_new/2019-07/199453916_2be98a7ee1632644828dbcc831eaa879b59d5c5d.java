package com.Constants;

import java.util.concurrent.TimeUnit;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

public class MPFPConstants 
{

	public static String url="http://dev.myplaceforparts.com/web/alliance/home";
	public static WebDriver driver =new FirefoxDriver();
	
	@BeforeTest
	public void LaunchApp() throws InterruptedException
	{
		driver.manage().timeouts().implicitlyWait(20, TimeUnit.SECONDS);
	    driver.manage().window().maximize();
		driver.get(url);
		Thread.sleep(2000);
		driver.findElement(By.xpath("//*[@value='Sign In']")).click();
		
	}
	
	@AfterTest
	public void closeApp()
	{
		driver.close();
	}
	
}