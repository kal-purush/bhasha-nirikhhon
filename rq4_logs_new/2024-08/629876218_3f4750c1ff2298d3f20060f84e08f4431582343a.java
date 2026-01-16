package com.Example.Testing;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;

public class App {
	public static void main(String[] args) throws InterruptedException {
		WebDriver driver = new ChromeDriver();
		driver.manage().window().maximize();
		// driver.get("https://devrpm.kavanant.com/");
		driver.get("https://www.facebook.com/login/");
		Thread.sleep(2000);
		driver.findElement(By.id("email")).sendKeys("de@gmail.com");
		Thread.sleep(2000);
		driver.findElement(By.id("pass")).sendKeys("de");
		Thread.sleep(2000);
		driver.findElement(By.xpath("//div[@class='_9lsa']")).click();
		Thread.sleep(2000);
		driver.findElement(By.xpath("//button[@id='loginbutton']")).click();
		Thread.sleep(2000);
		driver.findElement(By.xpath("//div[@class='_9kpm']")).click();

	}
}