package seleniumPractise.amruta;

import java.io.IOException;
import java.util.Properties;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;

import util.Init;
import util.PropFileOperation;

public class GmailLogin {

	public static void main(String[] args) throws IOException, InterruptedException {

		WebDriver driver = Init.initChromeDriver();
		Properties prop = PropFileOperation.loadProp();
		driver.get("https://www.gmail.com");
		System.out.println("Browser Open");
		// driver.manage().window().maximize();
		driver.findElement(By.id("identifierId")).sendKeys(prop.getProperty("email"));
		System.out.println("email address entered");
		// it will return two webelemnts
		// driver.findElement(By.xpath("//span[@class='RveJvd snByac']")).click();
		// driver.findElement(By.id("identifierNext")).click();
		driver.findElement(By.id("identifierNext")).submit();
		System.out.println("cliked on next");
		Thread.sleep(2000);
		driver.findElement(By.name("password")).sendKeys(prop.getProperty("password"));
		System.out.println("Entered password");
		driver.findElement(By.id("passwordNext")).click();
		System.out.println("logged in successfully");
		driver.close();
		System.out.println("browser closed");
	}

}