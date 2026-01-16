package dev.spp.login;

import java.util.concurrent.TimeUnit;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;

public class LoginLogout
{	
	public static void main(String[] args) throws InterruptedException
	{
		System.setProperty("webdriver.chrome.driver", "D:\\Selenium\\Chromedriver\\chromedriver.exe");
		WebDriver driver = new ChromeDriver();
		driver.get("http://ws51.rlndevbox.net/super_user");
		Thread.sleep(1000);
		driver.manage().window().maximize();
		driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);
		//login
		driver.findElement(By.id("user_username")).sendKeys("superuser");
		driver.findElement(By.id("user_password")).sendKeys("sppcloud@123");
		driver.findElement(By.id("sign_in")).click();
		
		//logout
		driver.findElement(By.xpath("a[href='/users/sign_out']")).click();
		
		
		Thread.sleep(1000);
		
		

	}
}