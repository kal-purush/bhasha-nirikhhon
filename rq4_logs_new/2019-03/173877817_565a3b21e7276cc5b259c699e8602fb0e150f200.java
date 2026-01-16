package seleniumPractise.radhika;

import org.openqa.selenium.Alert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;

import util.Init;

public class Q1Assignement {

	public static void main(String[] args) throws InterruptedException {
		WebDriver driver = Init.initChromeDriver();

		System.out.println("Browser Open");
		driver.navigate().to("http://automationbykrishna.com/#");
		System.out.println("URL Opened");
		driver.findElement(By.id("basicelements")).click();
		//((JavascriptExecutor)driver).executeScript("scroll(0,400)");

		driver.findElement(By.xpath("//input[@name='ufname'and @id='UserFirstName']")).sendKeys("Radhika");
		//driver.findElement(By.id("UserFirstName")).sendKeys("VEDIKA");
		System.out.println("Values passed ");
		driver.findElement(By.xpath("//input[@name='ulname'and @id='UserLastName']")).sendKeys("Modi");
		//driver.findElement(By.id("UserLastName")).sendKeys("MOdi");
		driver.findElement(By.xpath("//input[@name='cmpname'and @id='UserCompanyName']")).sendKeys("CTS");
		driver.findElement(By.xpath("//button[@class='btn btn-primary']")).click();
		Thread.sleep(5000);
		String s1="vedika and modi and Perscitus";
		Alert str=driver.switchTo().alert();
		String s2=str.getText();
		//System.out.println(str);
		if(s1.equals(s2)){
			
			System.out.println("Text is validated ");
			str.accept();
		}
		driver.close();

	}

	}

