package com.mark55.automation;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

import io.github.bonigarcia.wdm.WebDriverManager;

public class Automation {
	WebDriver driver;
	
	public Automation() {
		WebDriverManager.chromedriver().setup();
		driver =new ChromeDriver();
	}
	
	public void activate(String mailStr, String pwdStr) {
		driver.manage().window().maximize();
		driver.get("https://elearning.dei.unipd.it/mod/page/view.php?id=1673");
		WebElement img= driver.findElement(By.cssSelector("img[alt=\"Logo SSO Unipd\"]"));
		img.click();
		
		WebElement mail= driver.findElement(By.id("j_username_js"));
		WebElement password= driver.findElement(By.id("password")); 
		WebElement radio= driver.findElement(By.id("radio2")); 	
		WebElement button= driver.findElement(By.id("login_button_js")); 		
		mail.sendKeys(mailStr);
     	password.sendKeys(pwdStr);
     	radio.click();
     	button.click();
     	
     	//driver.quit();
	}
	
	
}