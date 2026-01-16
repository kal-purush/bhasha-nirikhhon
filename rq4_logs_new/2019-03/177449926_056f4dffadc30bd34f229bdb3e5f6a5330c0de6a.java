package page.objects;

import java.io.IOException;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.PageFactory;
import org.testng.Assert;

import utils.ReportingUtil;

public class AccountPage {
	
	WebDriver driver;
	ReportingUtil report;
	
	//Elements
	@FindBy(xpath="//h2[@class='side-nav-header'][text()='My Account']")
	private WebElement MyAccountHeaderText;
	
	@FindBy(id="signOut")
	private WebElement signOutLink;
	
	//constructor
	public AccountPage(WebDriver d, ReportingUtil ru) {
		driver=d;
		report = ru;
		PageFactory.initElements(driver, this);
	}
	
	//methods
	public void verifyMyAccountPageIsDisplayed() throws IOException {
		try {
			Assert.assertTrue(MyAccountHeaderText.isDisplayed());
			report.report("PASS", "Verify My Account page is displayed", "My Account page is displayed");
		}catch(AssertionError ae) {
			report.report("FAIL", "Verify My Account page is displayed", "My Account page is not displayed");
		}
	}
	
	public void clickSignOut(){
		signOutLink.click();
	}
}