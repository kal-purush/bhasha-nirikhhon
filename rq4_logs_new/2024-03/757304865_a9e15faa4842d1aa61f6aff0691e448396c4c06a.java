package ui_beambox_automation;

import java.time.Duration;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.Wait;
import org.openqa.selenium.support.ui.WebDriverWait;

public class BenefitsPages {
	private WebDriver driver;

	public BenefitsPages(WebDriver driver) {
		super();
		this.driver = driver;
	}

	public WebDriver getDriver() {
		return driver;
	}

	public void setDriver(WebDriver driver) {
		this.driver = driver;
	}
	
	public void smartMarkitingCampainPage() {
		driver.get("http://lvh.me:3000/");
		driver.manage().window().maximize();
		
		WebElement whyBeamboxLink = driver.findElement(By.xpath("//*[@id=\"nav\"]/div/div[2]/div/ul/li[1]/a"));
		System.out.println(whyBeamboxLink.getText());
		whyBeamboxLink.click();
		
		Wait<WebDriver> waitForSmartMarktingLink = new WebDriverWait(driver, Duration.ofSeconds(20));
		WebElement smartMarktingLink = waitForSmartMarktingLink.until(ExpectedConditions.elementToBeClickable(By.xpath("//*[@id=\"nav\"]/div/div[2]/div/ul/li[1]/div/div/div/div[1]/div/div[4]/a")));
		System.out.println(smartMarktingLink.getText());
		smartMarktingLink.click();
		
		String currentUrl = driver.getCurrentUrl();
		System.out.println(currentUrl);
		
		String utmParamsUrl = currentUrl+"?utm_source=google&utm_medium=search&utm_campaign=GS_ENG-Speaking_NB_Email_TCPA&utm_term=sms";
		driver.get(utmParamsUrl);
		
		
	}

}