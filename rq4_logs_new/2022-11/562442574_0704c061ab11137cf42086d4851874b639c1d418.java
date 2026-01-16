package org.alerts;

import org.openqa.selenium.Alert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

public class AlertWithTextBox {
	public static void main(String[] args) throws InterruptedException {
		System.setProperty("webdriver.chrome.driver",
				"C:\\Users\\Admin\\eclipse-workspace\\Selenium\\driver\\chromedriver.exe");
		WebDriver driver = new ChromeDriver();
		driver.manage().window().maximize();
		driver.get("https://demo.automationtesting.in/Alerts.html");

		WebElement alert = driver.findElement(By.xpath("//a[text()='Alert with Textbox ']"));
		alert.click();

		WebElement textalert = driver.findElement(By.xpath("//button[@class='btn btn-info']"));
		textalert.click();
		Alert a = driver.switchTo().alert();
		a.sendKeys("hi i am rosie");
		a.accept();

	}

}