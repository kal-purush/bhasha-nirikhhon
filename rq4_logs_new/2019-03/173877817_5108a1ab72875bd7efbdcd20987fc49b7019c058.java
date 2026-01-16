package seleniumPractise.technoCredits.browserNavigationWebelementAlertXpath;

import org.openqa.selenium.Alert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;

import util.Init;

public class AlertDemoPrac {

	public static void main(String[] args) throws InterruptedException {

		WebDriver driver = Init.initChromeDriver();
		driver.get("http://automationbykrishna.com/#");
		driver.findElement(By.linkText("Basic Elements")).click();
		// Alert alert1 = driver.switchTo().alert();
		// driver.findElement(By.partialLinkText("asic")).click();
		System.out.println("on basic element page");
		Thread.sleep(2000);
		driver.findElement(By.xpath("//input[contains(@id,'serFirstNa')]")).sendKeys("krishna");
		System.out.println("entered name");
		driver.findElement(By.xpath("//input[@name='ulname'] | //input[contains(@id,'erLastNam')]")).sendKeys("kanani");
		System.out.println("Entered last name");
		driver.findElement(By.xpath("//input[@class='form-control'][@id='UserCompanyName']")).sendKeys("Mobiliya");
		System.out.println("Entered company name");
		driver.findElement(By.xpath("//div[@name='secondSegment'][1]//button[text()='Submit']")).click();
		System.out.println("cliked on submit button");
		Thread.sleep(2000);
		Alert alert = driver.switchTo().alert();
		System.out.println(alert.getText());
		alert.accept();

		System.out.println("alert accepted");

	}

}