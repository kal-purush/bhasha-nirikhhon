package pageObjects.Sitecore;

import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

public class SCExhibitorsEventsApprovalsPage {

	public WebDriver driver;
	public WebDriverWait wait;

	By eventsApprovePageSeeMoreCTA = By.xpath("//button[@class='btn']"); // Locator for Digital Admin Panel
	By digitalAdminPanelSearch1stEmail = By.xpath("//table[@class='table table-bordered']/tbody[1]/tr[1]/td[2]"); // Locator for 1st Email id
	
	
	
	public SCExhibitorsEventsApprovalsPage(WebDriver driver) {
		// TODO Auto-generated constructor stub
		this.driver = driver;
	}

	public WebElement getdigitalAdminPanelSearch1stEmail() throws InterruptedException {
		wait = new WebDriverWait(driver, 30);
		wait.until(ExpectedConditions.elementToBeClickable(digitalAdminPanelSearch1stEmail));
		return driver.findElement(digitalAdminPanelSearch1stEmail);
	}
	public WebElement geteventsApprovePageSeeMoreCTA() throws InterruptedException {
		wait = new WebDriverWait(driver, 30);
		wait.until(ExpectedConditions.elementToBeClickable(eventsApprovePageSeeMoreCTA));
		return driver.findElement(eventsApprovePageSeeMoreCTA);
	}

}